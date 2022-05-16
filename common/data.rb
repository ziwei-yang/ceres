require_relative '../common/bootstrap' unless defined? URN::BOOTSTRAP_LOAD

module URN
	# Listen to shared market status events. (banned info)
	class MktStatusListener
		class << self
			include APD::LogicControl
			include URN::Misc
		end

		def self.redis
			URN::RedisPool
		end

		def self.log(*args)
			return unless @@verbose
			args.push({:level => 2})
			puts *args
		end

		@@_service_thread = nil
		@@verbose = true
		@@shared_status = Concurrent::Hash.new
		@@shared_status[:banned_by_mkt] ||= Concurrent::Hash.new # Support more types by adding slots here.
		def self.start_service
			unless @@_service_thread.nil?
				puts "MktStatusListener service is running already"
				return
			end
			@@_service_thread = :setup
			@@_service_thread = Thread.new(abort_on_exception:true) {
				Thread.current[:name] = "MktStatusListener"
				loop {
					begin
						_listen_redis()
					rescue => e
						puts e
					end
					sleep 1
				}
			}
		end

		# Query common data.
		def self.banned_util(market_name)
			info = @@shared_status.dig(:banned_by_mkt, market_name)
			if info.nil?
				info = _fetch_ban_status(market_name) # {} if not banned.
			end
			return info
		end

		def self.clear_banned(market_name)
			@@shared_status[:banned_by_mkt].delete(market_name)
		end

		def self.banned_util_set(market_name, json)
			@@shared_status[:banned_by_mkt][market_name] = json
		end

		def self._listen_redis(opt={})
			channel = "URANUS:status_channel"
			log "<< subscribing #{channel}"
			redis.subscribe(channel) { |on|
				on.subscribe { |chn, num|
					log "<< subscribed to #{chn} (#{num} subscriptions)"
				}
				on.message { |chn, msg| # Just fetch remote status again once got msg.
					log "<< #{chn} #{msg}"
					json = nil
					begin
						json = JSON.parse(msg)
					rescue
						log "<< #{chn} invalid json"
						next 
					end
					type, market, account = json['type'], json['market'], json['account']
					if type == 'ban' || type == 'ban_cleared'
						_fetch_ban_status(market, account: account, data: json)
					else
						log "<< #{chn} unknown type #{type}"
					end
				}
				on.unsubscribe { |chn, num|
					raise "Unsubscribed to ##{chn} (#{num} subscriptions)"
				}
			}
		end

		def self._fetch_ban_status(market_name, opt={})
			j = opt[:data]
			if j.nil? # Fetch data from key, if no-cache data given.
				key = "URANUS:#{market_name}:banned_info"
				str = endless_retry(sleep:1) { redis.get(key) }
				# Return empty when no data
				if str.nil?
					@@shared_status[:banned_by_mkt][market_name] = {}
					return {}
				end
				# Return empty when err data
				begin
					j = parse_json(str)
				rescue
					log "<< invalid json at #{key}"
					@@shared_status[:banned_by_mkt][market_name] = {}
					return {}
				end
			end
			time = j['time'] = DateTime.parse(j['time'])
			reason = j['reason']
			puts "<< #{market_name} banned util #{time} #{reason[0..299]}".red
			@@shared_status[:banned_by_mkt][market_name] = j
			return j
		end
	end

	# In mode :live and :dryrun
	# Market data subscriber that could also drive algo or just print data.
	# In mode :service, it would run in background and only provide data querying.
	class MktDataSource
		include URN::Misc
		class Parser # Wrap all market data parser into Parser.
			include URN::MarketData
			def initialize(mgr, market_snapshot, disable_markets=[], opt={})
				@client_mgr = mgr
				@market_snapshot = market_snapshot
				@disable_markets = disable_markets
				@_last_odbk_cache = {}
				@_last_tick_cache = {}
			end

			def market_client(mkt)
				@client_mgr.market_client(mkt)
			end
		end

		def redis
			URN::RedisPool
		end

		attr_reader :algo, :asset_mgr, :market_pairs
		def initialize(market_pairs, opt={}) # Redis data keys.
      @verbose = opt[:verbose] == true
			@market_pairs = market_pairs.clone.to_a
			@markets = @market_pairs.map { |mp| mp[0] }

			# For refreshing orderbooks, would be overwrited in service mode.
			@market_snapshot = {}
			# Any data updated after last refresh_orderbooks(), reset in refresh_orderbooks()
			@market_snapshot_chg = Concurrent::Hash.new
			@market_latest_trades = {}

			# Listen from redis
			@redis_sub_channels = {}
			@redis_tick_buffer = {}
			@redis_tick_chg = false

			@abandon_large_latency_data = opt[:max_latency] || -1 # give up data when latency > ?? ms

			# :dryrun :live :service
			@mode = opt[:mode] || :dryrun

			if @mode == :service
				@service_monitor_threads = Concurrent::Array.new
				@service_client_mgr = opt[:client_mgr] || raise("service mode needs client_mgr")
				@disable_markets = opt[:disable_markets] || []
				@market_given_names = opt[:market_given_names]
			else
				@mgr = opt[:client_mgr] || RawMarketManager.new
			end

			# Init market clients, prepare channels
			@market_client_map = opt[:market_client_map] || {}
			@market_pairs.each_with_index { |mp, i|
				mkt_given_name = nil
				mkt_given_name = @market_given_names[i] unless @market_given_names.nil?
				m, p = mp
				odbk_m = m.split('_')[0]
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"] ||= []
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"].push(
					[m, p, :odbk, mkt_given_name]
				)
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"] = @redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"].uniq

				if opt[:with_trades] == true # Also listen trades if set true
					@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"] ||= []
					@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"].push(
						[m, p, :tick, mkt_given_name]
					)
					@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"] = @redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"].uniq
				end

				next if @market_client_map[m] != nil
				c = nil
				if @mode == :service
					if mkt_given_name.nil?
						c = @service_client_mgr.market_client(m)
					else
						c = @service_client_mgr.market_client(mkt_given_name)
					end
					@market_client_map[mkt_given_name || m] = c
					next
				end
				if @mode == :dryrun
					puts "Initializing dummy market #{m} #{p} for deviation evaluation"
					c = DummyMarketClient.new(m, verbose:true, debug:true)
				else
					puts "Initializing market #{m} #{p} for deviation evaluation".blue
					c = @mgr.market_client(m, create_on_miss:true, create_opt:{ :trade_mode => 'no' })
					c.preprocess_deviation_evaluate(p)
				end
				@market_client_map[m] = c
			}

			@parser = Parser.new(
				@service_client_mgr || @mgr, @market_snapshot, @disable_markets
			)
			@debug = opt[:debug] == true
			@verbose = opt[:verbose] == true
			@spin_chars = '\|/-'
			@work_ct = 0
		end

    def inspect
      self.class.name
    end

		# Only support one algo now.
		# Very rare senario to run multi algos in one data source thread.
		def drive(algo_class)
			raise "Error mode #{@mode}" if [:dryrun, :live, :backtest].include?(@mode) == false
			raise "@algo has been set" unless @algo.nil?
			if algo_class.is_a?(URN::MarketAlgo) || algo_class.is_a?(URN::MultiMarketAlgo)
				@algo = algo_class
				puts "Will drive algo #{algo_class.class.name}"
			elsif algo_class.is_a?(Class)
				@algo = algo_class.new(@market_pairs)
				puts "Will drive #{algo_class.name} market_pairs:#{@market_pairs}"
			else
				raise "Unknown argument. #{algo_class}"
			end
			# Set aggressive redis pool strategy
			redis.pool.keep_avail_size = 5 if redis() == URN::RedisPool
			@algo.mgr = @mgr
			@algo.start()
		end

		def _listen_redis()
			Thread.current.priority = 1
			Thread.current[:name] = "DataSource #{@market_pairs.to_json[0..39]}"
			puts "Subscribing #{JSON.pretty_generate(@redis_sub_channels.keys)}".green
			return if @redis_sub_channels.empty?
			@msg_t = nil
			redis.subscribe(*(@redis_sub_channels.keys)) { |on|
				on.subscribe { |chn, num|
					puts "Subscribed to #{chn} (#{num} subscriptions)" if @market_pairs.size < 5
				}
				msg_ct = 0
				slow_msg_ct = 0
				giveup_notify_time = 0
				on.message { |chn, msg|
					msg_ct += 1
					start_t = Time.now.to_f
					start_ms = start_t * 1000
					data_mkt_t, data_t = 0, 0
					data_type = nil
					msg_interval_t = (@msg_t.nil? ? 0 : (start_t - @msg_t)).round(3)
					@msg_t = start_t
					local_time_diff = 0
					mkt_time_diff = 0

          puts msg if @verbose
					# Parse data, msg should contains all data.
					msg = parse_json(msg)
					parse_ms = Time.now.to_f * 1000 - start_ms # 0.0X ms

					changed_odbk, new_mkt_trades = nil, nil
					changed_mp = [] # Might be mapping to different mp
					msg_giveup = false
					@redis_sub_channels[chn].each { |mptn|
						m, p, type, mkt_given_name = mptn
						mp = [m, p]

						# Too much time cost here would lead data updates falling behind
						begin
							# If latency is high and msg keep piling up.
							if msg_interval_t <= 0.1 && @abandon_large_latency_data > 0
								latency = 0
								if type == :odbk
									bids, asks, t, mkt_t = msg
									latency = start_ms-t
								elsif type == :tick
									trades, t = msg
									latency = start_ms-t
								end
								if latency >= @abandon_large_latency_data
									slow_msg_ct += 1
									ratio = slow_msg_ct.to_f/msg_ct
									# print everytime would cause severe latency, do it wisely.
									if ratio > 0.1 && start_t - giveup_notify_time > 5
										puts "Slow channel msg #{slow_msg_ct} #{(ratio*100).round(2)}% latency #{latency.round} > #{@abandon_large_latency_data}ms"
										giveup_notify_time = start_t
									end
									if msg_ct >= 100_000 #RESET count
										msg_ct == 0
										slow_msg_ct == 0
									end
									# Only giveup orderbook message.
									if type == :odbk
										msg_giveup = true
										# clear data that has high latency.
										# Mark snapshot data as empty, not usable. Don't mark as nil, might cause null pointer somewhere.
										@market_snapshot[mkt_given_name || m] ||= Concurrent::Hash.new
										@market_snapshot[mkt_given_name || m][:orderbook] = [[], [], 0, 0]
										@market_snapshot[mkt_given_name || m][p] ||= Concurrent::Hash.new
										@market_snapshot[mkt_given_name || m][p][:orderbook] = [[], [], 0, 0]
										# Also mark market_snapshot_chg, remeber to clear data in refresh_orderbooks()
										@market_snapshot_chg[[mkt_given_name || m, p]] = true
										break # Stop any processing about this msg.
									end
								end
							end

							if type == :odbk
								# @market_snapshot[mkt_given_name][:orderbook] and @market_snapshot[mkt_given_name][p][:orderbook] would be updated.
								data_chg = @parser.refresh_orderbooks(
									[@market_client_map[mkt_given_name || m]],
									[p],
									@market_snapshot,
									data: [msg],
									no_real_p:true
								)
								next unless data_chg
								changed_odbk = @market_snapshot[mkt_given_name || m][:orderbook]
								@market_snapshot_chg[[mkt_given_name || m, p]] = true
								changed_mp.push(mp)
								bids, asks, t, mkt_t = changed_odbk
								data_mkt_t = mkt_t
								data_t = t
								data_type = type
							elsif type == :tick
								trades, t = msg
								new_mkt_trades = @parser.parse_new_market_trades(m, p, trades)
								if new_mkt_trades.size > 0
									changed_trades = new_mkt_trades
									changed_mp.push(mp)
								end
								data_t = t
								data_type = type
								@market_latest_trades[mkt_given_name || m] ||= []
								@market_latest_trades[mkt_given_name || m].concat(new_mkt_trades)
								size = @market_latest_trades[mkt_given_name || m].size
								if size > 100
									@market_latest_trades[mkt_given_name || m].slice!(0, size-100)
								end
							end
						rescue => e
							APD::Logger.error e
							next
						end
					}

					return if @_should_exit
					next if msg_giveup
					if @mode == :service
						@service_monitor_threads.each { |thread| thread.wakeup }
						next
					end

					# Below code for mdoe :dryrun and :live
					# Print stat info inline and drive algo.

					now = Time.now.to_f
					local_time_diff = (now*1000 - data_t.to_f).round(3)
					cost_ms = ((now - start_t)*1000).round(3)
					mkt_time_diff = (now*1000 - data_mkt_t.to_f).round(3) if data_type == :odbk

					# Data might need to be mapping to different market-pair
					# Only select first market-pair to notify. In non-ab3 algo, this should be fine.
					m, p, type, mkt_given_name = mptn = @redis_sub_channels[chn][0]

					# Prepare stat_line
					@_stat_line = [
						m, # type,
						(cost_ms+local_time_diff).to_s[0..3].ljust(4),
					]
					if type == :odbk && data_mkt_t != nil
						@_stat_line.push(mkt_time_diff.to_i.to_s+'ms')
						# @_stat_line.push("<#{msg_interval_t.to_s[0..3].ljust(4)}")
						puts "SL-DT #{@_stat_line.join(' ')}" if local_time_diff > 30 || mkt_time_diff > 300
					elsif type == :tick
						# @_stat_line.push("[#{new_mkt_trades.size}]")
						# puts @_stat_line.join(' ')
					end

					# Notify algo.
					if @algo != nil
						if type == :odbk
							@algo.on_odbk(m, p, changed_odbk, stat_line:@_stat_line)
						elsif type == :tick
							@algo.on_tick(m, p, new_mkt_trades, stat_line:@_stat_line)
						end
					else
						puts "#{@_stat_line.join(' ')} ", nohead:true, inline:true, nofile:true
					end
				}
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			}
		end

		# Start in main thread mode, it keep driving data events.
		def start
			raise "Error mode #{@mode}" if @mode != :dryrun && @mode != :live
			Signal.trap("INT") {
				puts "SIGINT caught"
				@_should_exit = true
			}

			begin
				_listen_redis()
			rescue => e
				puts e
			end

			if @algo != nil && @algo.mode == :live
				@algo.finish()
				puts "saving algo"
				@algo.save_state_sync()
				puts "printing algo"
				@algo.print_info_sync()
				3.times { |i| # Let other threads exit.
					puts "data.start() would return in #{3-i}"
					sleep 1
				}
			end
		end

		# Act as URN::MarketDataAgent
		def start_service
			raise "Error mode #{@mode}" if @mode != :service
			raise "service thread is running" unless @_service_thread.nil?
			@_service_thread = :setup
			@_service_thread = Thread.new(abort_on_exception:true) {
				loop {
					begin
						_listen_redis()
					rescue => e
						puts e
					end
				}
			}
      if @_service_supervisor_thread.nil?
        @_service_supervisor_thread = Thread.new(abort_on_exception:true) {
					Thread.current[:name] = "DataSource #{@market_pairs.to_json[0..39]} supervisor"
          loop {
            now = Time.now.to_f
            @msg_t ||= now
						if @_service_thread.nil?
							;
            elsif now- @msg_t > 60
              puts "msg_t #{now-@msg_t} s ago, restart_service".white.on_red
              @msg_t = now
              restart_service()
            elsif now - @msg_t > 10
              puts "msg_t #{now-@msg_t} s ago".red
            elsif @verbose
              puts "msg_t #{now-@msg_t} s ago".green
            else
              ; # puts "msg_t #{now-@msg_t} seconds ago".green
            end
            sleep 1
          }
        }
      end
		end

		def stop_service
			if @_service_thread.is_a?(Thread)
				@_service_thread.exit
				puts "service thread exit()".white.on_red
        loop {
          status = @_service_thread.status
          puts "service thread status #{status}".white.on_red
          break if status == false || status.nil?
          keep_sleep 0.1
        }
				@_service_thread = nil
			else
				puts "No service thread to stop".red
				@_service_thread = nil
			end
		end

    def restart_service
      stop_service()
      start_service()
    end

		################################################
		# Service mode functions below.
		# To replace static redis methods in MarketData
		################################################
		# Keep same as MarketData
		def valid_markets(mkts)
			@parser.valid_markets(mkts)
		end
		def valid_markets_precompute(mkts)
			@parser.valid_markets_precompute(mkts)
		end
		def valid_markets_int(mkts)
			@parser.valid_markets_int(mkts)
		end
		def all_market_data_ready?(markets)
			missed_mkts = valid_markets(markets) - @market_snapshot.keys()
			return true if missed_mkts.empty?
			puts "Market data #{missed_mkts} is not ready."
			return false
		end
		# Try refreshing orderbooks, return if data changed.
		# Otherwise, wait util data changed then return new data.
		def wait_new_orderbooks(mkt_clients, pair_list, snapshot, opt={})
			data_chg_mp = refresh_orderbooks(mkt_clients, pair_list, snapshot, opt)
			if data_chg_mp[0] != nil
				return data_chg_mp if opt[:need_changed_mp]
				return true
			end

			data_chg_mp = []
      max_timeout = opt[:max_timeout]
			@service_monitor_threads.push(Thread.current)
      start_t = Time.now.to_f
			loop {
        if max_timeout.nil?
          sleep # Tick updates would wakeup thread too.
        else
          sleep max_timeout
        end
				data_chg_mp = refresh_orderbooks(mkt_clients, pair_list, snapshot, opt)
				break if data_chg_mp[0] != nil
        elasped_t = Time.now.to_f - start_t
        break if max_timeout != nil && elasped_t >= max_timeout
			}
			@service_monitor_threads.delete(Thread.current)
			if opt[:need_changed_mp]
				return data_chg_mp
			else
				return data_chg_mp[0] != nil
			end
		end
		# Return directly from @snapshot
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		# if opt[:no_real_p] is true, stop computing real price for better speed.
		def refresh_orderbooks(mkt_clients, pair_list, snapshot, opt={})
			data_by_mp = (opt[:data_by_mp] == true)
			@_valid_warning ||= {}
			start_ms = Time.now.to_f * 1000
			now = start_ms.to_i
			pair_ct = 0
			cp_list = mkt_clients.zip(opt[:order_pairs] || pair_list)
			ttl_pair_ct = cp_list.size
			# parsing_lmd will be applied on cp_list
			parsing_lmd = lambda { |d|
				client, pair = d
				raise "Pair is null" if pair.nil?
				m = client.given_name()
				mkt_name = client.market_name()
				if data_by_mp
					snapshot[[m,pair]] ||= {}
				else
					snapshot[m] ||= {}
				end
				# only re-calculate data those updated
				if @market_snapshot_chg[[m, pair]] == true
					@market_snapshot_chg[[m, pair]] = false # reset updated flag, delete() might cause free() error
				else
					next
				end
				# odbk = @market_snapshot.dig(m, :orderbook).clone # snapshot keeps changing
				odbk = @market_snapshot.dig(m, pair, :orderbook).clone # snapshot keeps changing
				next if odbk.nil?
				bids, asks, t, mkt_t = odbk
				next if bids.empty? && asks.empty? && t == 0 && mkt_t == 0 # Marked as unusable because of latency
				pair_ct += 1
				if opt[:max_depth] != nil # Filter top depth odbk.
					max_depth = opt[:max_depth]
					bids = bids[0..(max_depth-1)]
					asks = asks[0..(max_depth-1)]
					odbk = [bids, asks, t, mkt_t]
				end
				# Add real price with commission to each order
				if opt[:no_real_p] != true
					@parser._preprocess_orderbook(pair, odbk, client)
				end
				if opt[:fill_full_info] == true
					odbk[0..1].each { |orders|
						orders.each { |o|
							o['s'] = o['s'].to_f;
							o['p'] = o['p'].to_f;
							o['market'] = mkt_name;
							o['pair'] = pair
						}
					}
				end
				if data_by_mp
					old_odbk = snapshot[[m,pair]][:orderbook]
					snapshot[[m,pair]][:orderbook] = odbk
				else
					old_odbk = snapshot[m][:orderbook]
					snapshot[m][:orderbook] = odbk
				end
				next [m, pair] if old_odbk.nil?
				next if old_odbk == odbk
				bids, asks, t, mkt_t = odbk
				# Abort if timestamp is too old compared to system timestamp.
				abort_reason = nil
				gap = now - t.to_i
				abort_reason = "#{gap/1000} seconds ago" if gap > 60*1000
				if abort_reason != nil
					m = client.market_name()
					@_valid_warning[m] ||= 0
					if now - @_valid_warning[m] > 10*1000
						puts "#{m} #{pair} odbk -X-> data_chg, #{abort_reason}".red
						@_valid_warning[m] = now
					end
					next
				end
				# Check market timestamp with latest market_client timestamp.
				time_legacy = client.last_operation_time.to_i - t
				if time_legacy >= 0
					m = client.market_name()
					puts "#{m} #{pair} orderbook is #{time_legacy}ms old"
					next
				end
				next [m, pair]
			}
			# Parallel map can not boost up speed here, may also cause snapshot concurrent problems.
			# data_chg_mp = Parallel.map(cp_list, in_threads: 3, &parsing_lmd).select { |r| r != nil }
			data_chg_mp = cp_list.map(&parsing_lmd).select { |r| r != nil }
			end_ms = Time.now.to_f * 1000
			cost_ms = end_ms - start_ms
			if (cost_ms >=3 || pair_ct >= 5) && cost_ms*15 >= pair_ct # Expect 0.02~0.06 ms for 1 pair
				avg_t = (cost_ms/pair_ct).round(3)
				puts "#{cost_ms.round(3)} ms cost in parsing #{pair_ct}/#{ttl_pair_ct} pairs odbk, avg_t #{avg_t}"
			end
			data_chg_mp
		end
		def refresh_trades(mkt_clients, pair_list, snapshot, opt={})
			data_by_mp = (opt[:data_by_mp] == true)
			mkt_clients.zip(opt[:order_pairs] || pair_list).each { |client, pair|
				raise "Pair is null" if pair.nil?
				m = client.given_name()
				trades_his = @market_latest_trades[m]
				next if trades_his.nil?
				next if trades_his.empty?
				if data_by_mp
					snapshot[[m,pair]][:trades] = [trades_his, trades_his[0]['t'].to_i]
				else
					snapshot[m][:trades] = [trades_his, trades_his[0]['t'].to_i]
				end
			}
			true
		end
	end

	class HistoryMktDataSource < URN::MktDataSource
		def initialize(market_pairs, opt={}) # Redis data keys.
			@debug = opt[:debug] == true
			@verbose = opt[:verbose] == true
			@file_filter = opt[:file_filter]
			@his_data_dir = opt[:his_data_dir] || "#{URN::ROOT}/data/subscribe"
			@mgr = URN::DummyAssetManager.new(verbose:@verbose, debug:@debug)
			@market_snapshot = {}
			@market_clients = market_pairs.to_a.map do |mp|
				m, p = mp
				puts "Initializing dummy market #{m} #{p} for asset manager"
				@mgr.market_client(m)
			end
			@market_pairs = market_pairs.clone
			@debug = opt[:debug] == true
			@mode = :backtest
			@start_time = opt[:start_time]

			# Data frame: [type, data(contains timestamp)]
			@current_data_frame = {}
			@next_data_frame = {}

			@parser = Parser.new(@mgr, {}, [])

			_prepare_his_file()
		end

		def filename
			File.basename(@target_f)
		end

		def _prepare_his_file
			@market_pairs_files = {}
			@market_pairs_fopen = {}
			@market_pairs.each do |m, p|
				ramfiles = Dir["/mnt/ramdisk/#{m}_#{p}.*"].sort
				macramfiles = Dir["/Volumes/RAMDisk/#{m}_#{p}.*"].sort
				files = Dir["#{@his_data_dir}/#{m}_#{p}.*"].sort
				files = ramfiles + macramfiles + files
				if @file_filter != nil
					files = files.select { |f| f.include?(@file_filter) }
				end
				raise "No his files for #{m} #{p} #{@file_filter}" if files.empty?
				puts [m, p, "files:", files.size]
				files.each { |f| puts f }
				mp = [m,p]
				@market_pairs_files[mp] = files
				f = files.first
				@target_f = f
				if f.end_with?('.gz')
					@market_pairs_fopen[mp] = Zlib::GzipReader.open(f)
				else
					@market_pairs_fopen[mp] = File.open(f)
				end
				# Fill current_data_frame for first time
				@current_data_frame[mp] = _io_read_next_frame(mp)
				@next_data_frame[mp] = _io_read_next_frame(mp)
			end
		end

		def _io_read_next_frame(key)
			reader = @market_pairs_fopen[key]
			begin
				type = reader.readline()
				frame = parse_json(reader.readline())
				return [type, frame]
			rescue => e
				puts "Data end: #{e.class} #{e.message}"
				@finished = true
				return nil
			end
		end

		def _run_historical_files
			# Select the oldest data in next_data_frame, replace into current_data_frame.
			# Oldest data might be multiple.
			next_data_kv = @next_data_frame.to_a.sort_by { |kv| kv[1][1].last }.first
			next_data_key, next_data = next_data_kv
			next_data_t = next_data[1].last
			puts "next_data_t #{format_millisecond(next_data_t)}" if @debug
			changed_trades = {}
			changed_odbks = {}
			odbk_changed = false
			@next_data_frame.to_a.each do |mp, data|
				m, p = mp
				type, msg = data
				t = msg.last
				# Load more data into next_data_frame.
				if next_data_t >= t
					puts "Fill #{mp} #{type.inspect} #{format_millisecond(t)}" if @debug
					@current_data_frame[mp] = data
					@next_data_frame[mp] = _io_read_next_frame(mp)
					if @start_time != nil && next_data_t < @start_time
						;
					elsif type == "odbk\n"
						@mgr.market_client(m).update_odbk(p, msg)
						changed_odbks[mp] = msg
						odbk_changed = true
					elsif type == "t\n"
						@mgr.market_client(m).update_tick(p, msg)
						# Only needs trades, without mkt_t
						changed_trades[mp] = @parser.parse_new_market_trades(m, p, msg[0])
					else
						raise "Unknown type #{type.inspect}"
					end
				end
			end
			if @algo
				@algo.on_odbk(changed_odbks) if odbk_changed
				@algo.on_tick(changed_trades) if changed_trades.size > 0
				@finished ||= @algo.should_end_backtest?
			end
		end

		include URN::CLI
		def start
			if @algo.nil?
				puts "HistoryMktDataSource start without algo".red
			else
				puts "HistoryMktDataSource start with #{@algo.name}"
			end
			ct = 0
			history_start_t = @current_data_frame.values.first[1].last
			start_t = Time.now.to_f
			seg_t = Time.now.to_f
			seg_n = 50_000
			loop do
				if ct % seg_n == 0
					end_t = Time.now.to_f
					history_end_t = @current_data_frame.values.first[1].last
					history_span_hr = (history_end_t - history_start_t)/3600_000
					speed_h = '-'
					speed_h = (history_span_hr/(end_t-start_t)).round(2) if end_t != start_t
					speed_l = '-'
					speed_l = (seg_n/(end_t-seg_t)/1000).round if end_t != seg_t
					history_span_hr = history_span_hr.to_i
					if @algo.nil?
						puts [
							"#{(end_t-start_t).round}s",
							ct.to_s,
					 		"#{history_span_hr} hrs",
						 	"#{speed_h}/s",
							"#{speed_l}K/s"
						].join(', ')
					else
						@algo.compute_pnl()
						stat = @algo.stat
						name = @algo.name
						filled_o = (stat[:filled_buy]||0) + (stat[:filled_sell]||0)
						total_o = (stat[:dead_buy]||0) + (stat[:dead_sell]||0)
						stoploss_o = (stat[:stoploss_fill_latest]||0) + (stat[:stoploss_fil_his]||0)
						cancel_success_o = (stat[:cancel_success] || 0)
						cancel_total_o = cancel_success_o + (stat[:cancel_failed] || 0)
						print [
							"#{(end_t-start_t).round}s",
						 	"#{history_span_hr.round}hrs",
							"#{speed_h}/s",
							"#{speed_l}K/s",
							name,
							"PnL:#{(stat[:pnl] || 0).round(5)}",
							"SL:#{stat[:stoploss_fill_latest]}/#{stoploss_o}",
							"F:#{filled_o}/#{total_o}",
							"C:#{cancel_success_o}/#{cancel_total_o}",
							"tk:#{stat[:taker_ct]}",
							"pos:#{(@algo.position/@algo.maker_size).to_i}",
							"#{stat[:mkt_price]}",
							"\n"
						].join(' ')
						puts stat if @verbose
					end
					seg_t = Time.now.to_f
				end
				ct += 1
# 				break if ct == 50 # Fast test
#  				break if ct == 5_000 # Fast test
#  				break if ct == 50_000 # Fast test
				_run_historical_files()
				break if @finished
			end
			end_t = Time.now.to_f
			history_end_t = @current_data_frame.values.first[1].last
			history_span_hr = (history_end_t - history_start_t)/3600_000
			speed_h = (history_span_hr/(end_t-start_t)).round(2)
			puts "#{(end_t-start_t).round} s, #{ct} msg, #{history_span_hr} hrs, #{speed_h} hrs/s"
			if @algo
				stat = {:name=>@algo.name}.merge(@algo.stat())
				puts JSON.pretty_generate(stat).blue
				@algo.finish()
				return [stat]
			end
			if @mgr
				puts JSON.pretty_generate(@mgr.stat)
				if @mgr.stat.values[0][:alive] > 2
					@mgr.print_stat
					raise "Unexpected: 2 alive orders"
				end
			end
		end
	end
end

if __FILE__ == $0 && defined? URN::BOOTSTRAP_LOAD
	puts "Run MktDataSource"
	mds = URN::MktDataSource.new({
		'Binance' => 'USDT-BTC'
	}, verbose:true, debug:true, mode: :dryrun)
	mds.start
end
