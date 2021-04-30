require_relative '../common/bootstrap' unless defined? URN::BOOTSTRAP_LOAD

module URN
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

		attr_reader :algo, :asset_mgr
		def initialize(market_pairs, opt={}) # Redis data keys.
			@market_pairs = market_pairs.clone.to_a
			@markets = @market_pairs.map { |mp| mp[0] }
			@pairs = @market_pairs.map { |mp| mp[1] }
			@mgr = URN::StandardMarketManager.new(
				@markets, [],
				wait:opt[:wait], pair_prefix:opt[:pair_prefix]
			)

			# For refreshing orderbooks, would be overwrited in service mode.
			@market_snapshot = {}
			@market_latest_trades = {}

			# Listen from redis
			@redis_sub_channels = {}
			@redis_tick_buffer = {}
			@redis_tick_chg = false

			# :dryrun :live :service
			@mode = opt[:mode] || :dryrun

			if @mode == :service
				@service_monitor_threads = Concurrent::Array.new
				@service_client_mgr = opt[:client_mgr] ||
					raise("service mode needs client_mgr")
				@disable_markets = opt[:disable_markets] || []
				@market_given_names = opt[:market_given_names]
			end

			# Init market clients, prepare channels
			@market_client_map = opt[:market_client_map] || {}
			@market_pairs.each_with_index { |mp, i|
				mkt_given_name = nil
				mkt_given_name = @market_given_names[i] unless @market_given_names.nil?
				m, p = mp
				odbk_m = m.split('_')[0]
				# Listen orderbook only.
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"] ||= []
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_odbk_channel"].push(
					[m, p, :odbk, mkt_given_name]
				)
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"] ||= []
				@redis_sub_channels["URANUS:#{odbk_m}:#{p}:full_tick_channel"].push(
					[m, p, :tick, mkt_given_name]
				)
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
					c = @mgr.market_client(m, create_on_miss:true, create_opt:{})
					c.preprocess_deviation_evaluate(p)
				end
				@market_client_map[m] = c
			}

			@parser = Parser.new(
				@service_client_mgr, @market_snapshot, @disable_markets
			)
			@debug = opt[:debug] == true
			@verbose = opt[:verbose] == true
			@spin_chars = '\|/-'
			@work_ct = 0
		end

		# Only support one algo now.
		# Very rare senario to run multi algos in one data source thread.
		def drive(algo_class)
			raise "Error mode #{@mode}" if [:dryrun, :live, :backtest].include?(@mode) == false
			raise "@algo has been set" unless @algo.nil?
			if algo_class.is_a?(URN::MarketAlgo)
				@algo = algo_class
				puts "Will drive algo #{algo_class.class.name}"
			elsif algo_class.is_a?(Class)
				@algo = algo_class.new(@market_pairs.to_h)
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
			Thread.current[:name] = "DataSource #{@market_pairs} redis"
			puts "Subscribing #{@redis_sub_channels.keys}"
			@msg_t = nil
			redis.subscribe(*(@redis_sub_channels.keys)) do |on|
				on.subscribe { |chn, num| puts "Subscribed to ##{chn} (#{num} subscriptions)" }
				on.message do |chn, msg| # TODO check if channle is orderbook or tick.
					start_t = Time.now.to_f
					data_mkt_t, data_t = 0, 0
					data_type = nil
					msg_interval_t = (@msg_t.nil? ? 0 : (start_t - @msg_t)).round(3)
					@msg_t = start_t
					local_time_diff = 0
					mkt_time_diff = 0

					# Parse data, msg should contains all data.
					msg = parse_json(msg)

					@redis_sub_channels[chn].each { |mptn|
						m, p, type, mkt_given_name = mptn
						mp = [m, p]

						# Too much time cost here would lead data updates falling behind
						changed_odbk = nil
						new_mkt_trades = nil
						begin
							if type == :odbk
								data_chg = @parser.refresh_orderbooks(
									[@market_client_map[mkt_given_name || m]],
									[p],
									@market_snapshot,
									data: [msg],
									no_real_p:true
								)
								next unless data_chg
								changed_odbk = @market_snapshot[mkt_given_name || m][:orderbook]
								bids, asks, t, mkt_t = changed_odbk
								data_mkt_t = mkt_t
								data_t = t
								data_type = type
							elsif type == :tick
								trades, t = msg
								new_mkt_trades = @parser.parse_new_market_trades(m, p, trades)
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

					# Prepare stat_line
					m, p, type, mkt_given_name = mptn = @redis_sub_channels[chn][0]
					@_stat_line = [
						m, type,
						cost_ms.to_s[0..3].ljust(4),
						local_time_diff.to_s[0..3].ljust(4)
					]
					if type == :odbk
						@_stat_line.push(mkt_time_diff.to_s[0..3].ljust(4))
						# @_stat_line.push("<#{msg_interval_t.to_s[0..3].ljust(4)}")
						puts "SL-DT #{@_stat_line.join(' ')}" if local_time_diff > 30 || mkt_time_diff > 50
					elsif type == :tick
						# @_stat_line.push("[#{new_mkt_trades.size}]")
						# puts @_stat_line.join(' ')
					end

					# Notify algo.
					if @algo != nil
						mp = [m, p]
						if type == :odbk
							@algo.on_odbk({mp => changed_odbk}, stat_line:@_stat_line)
						elsif type == :tick
							@algo.on_tick({mp => new_mkt_trades}, stat_line:@_stat_line)
						end
					else
						puts "#{@_stat_line.join(' ')} ", nohead:true, inline:true, nofile:true
					end
				end
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			end
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
			data_chg = refresh_orderbooks(mkt_clients, pair_list, snapshot, opt)
			return true if data_chg
			@service_monitor_threads.push(Thread.current)
			loop {
				sleep # Tick updates would wakeup thread too.
				data_chg = refresh_orderbooks(mkt_clients, pair_list, snapshot, opt)
				break if data_chg
			}
			@service_monitor_threads.delete(Thread.current)
			return true
		end
		# Return directly from @snapshot
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		# if opt[:no_real_p] is true, stop computing real price for better speed.
		def refresh_orderbooks(mkt_clients, pair_list, snapshot, opt={})
			@_valid_warning ||= {}
			data_chg = false
			now = (Time.now.to_f*1000).to_i
			mkt_clients.zip(opt[:order_pairs] || pair_list).each do |client, pair|
				raise "Pair is null" if pair.nil?
				m = client.given_name()
				mkt_name = client.market_name()
				snapshot[m] ||= {}
				odbk = @market_snapshot.dig(m, :orderbook).clone # snapshot keeps changing
				next if odbk.nil?
				if opt[:max_depth] != nil # Filter top depth odbk.
					max_depth = opt[:max_depth]
					bids, asks, t, mkt_t = odbk
					bids = bids[0..max_depth]
					asks = asks[0..max_depth]
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
				old_odbk = snapshot[m][:orderbook]
				snapshot[m][:orderbook] = odbk
				next (data_chg = true) if old_odbk.nil?
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
				time_legacy = client.last_operation_time.strftime('%Q').to_i - t
				if time_legacy >= 0
					m = client.market_name()
					puts "#{m} orderbook is #{time_legacy}ms old"
					next
				end
				data_chg = true
			end
			data_chg
		end
		def refresh_trades(mkt_clients, pair_list, snapshot, opt={})
			mkt_clients.zip(trade_his_list, pair_list).each { |client, trade_his, pair|
				m = mkt_client.given_name()
				trades_his = @market_latest_trades[m]
				next if trades_his.nil?
				snapshot[m][:trades] = trade_his
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
			@pairs = market_pairs.keys
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
		'Bybit' => 'USD-BTC@P'
	}, verbose:true, debug:true)
	mds.start
end
