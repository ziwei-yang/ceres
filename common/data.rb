require_relative '../common/bootstrap' unless defined? URN::BOOTSTRAP_LOAD

module URN
	# Market data subscriber that could also drive algo or just print data.
	class MktDataSource
		include URN::MarketData
		attr_reader :algo, :asset_mgr
		def initialize(market_pairs, opt={}) # Redis data keys.
			@market_pairs = market_pairs.clone
			@markets = @market_pairs.keys
			@pairs = @market_pairs.values
			@mgr = URN::StandardMarketManager.new(@markets)

			# For refreshing orderbooks.
			@market_snapshot = {}
			@market_status_cache = {}

			# Listen from redis
			@redis_sub_channels = {}
			@redis_tick_buffer = {}
			@redis_tick_chg = false

			# :dryrun :live
			@mode = opt[:mode] || :dryrun

			@market_client_map = {}
			@market_clients = @market_pairs.to_a.map do |mp|
				m, p = mp
				# Listen orderbook only.
				@redis_sub_channels["URANUS:#{m}:#{p}:full_odbk_channel"] = mp
				c = nil
				if @mode == :dryrun
					puts "Initializing market #{m} #{p} for asset manager"
					c = DummyMarketClient.new(m, verbose:true, debug:true)
				else
					puts "Initializing market #{m} #{p} for asset manager"
					c = @mgr.market_client(m, create_on_miss:true, create_opt:{})
					c.preprocess_deviation_evaluate(p)
				end
				@market_client_map[m] = c
				c
			end

			@debug = opt[:debug] == true
			@verbose = opt[:verbose] == true
			@spin_chars = '\|/-'
			@work_ct = 0
		end

		# Only support one algo now.
		# Very rare senario to run multi algos in one data source thread.
		def drive(algo_class)
			raise "@algo has been set" unless @algo.nil?
			if algo_class.is_a?(URN::MarketAlgo)
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
			Thread.current[:name] = "DataSource #{@market_pairs} redis"
			puts "Subscribing #{@redis_sub_channels.keys}"
			redis.subscribe(*(@redis_sub_channels.keys)) do |on|
				on.subscribe { |chn, num| puts "Subscribed to ##{chn} (#{num} subscriptions)" }
				on.message do |chn, msg| # TODO check if channle is orderbook or tick.
					m, p = mp = @redis_sub_channels[chn]

					# Parse data, msg should contains all data.
					start_t = Time.now.to_f
					msg = parse_json(msg)
					# Too much time cost here would lead data updates falling behind
					data_chg = refresh_orderbooks(
						[@market_client_map[m]],
						[p],
						@market_snapshot,
						data: [msg],
						no_real_p:true,
						cache: @market_status_cache
					)
					cost_t = ((Time.now.to_f - start_t)*1000).round(3)
					next unless data_chg
					if @verbose
						now = Time.now.to_f
						changed_odbk = @market_snapshot[m][:orderbook]
						bids, asks, t, mkt_t = changed_odbk
						local_time_diff = (now*1000 - t.to_f).round(3)
						mkt_time_diff = (now*1000 - mkt_t.to_f).round(3)
						@_stat_line = [
							m.ljust(8),
							'odbk', cost_t.to_s.ljust(6),
							'lag', local_time_diff.to_s.ljust(6), mkt_time_diff.to_s.ljust(6)
						]
						if local_time_diff > 30 || mkt_time_diff > 60
							puts @_stat_line.join(' ')
						end
					end

					# Notify algo.
					if @algo != nil
						changed_odbk = @market_snapshot[m][:orderbook]
						@algo.on_odbk({ mp => changed_odbk}, stat_line:@_stat_line)
					else
						puts "#{@_stat_line.join(' ')}    ", nohead:true, inline:true, nofile:true
					end
				end
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			end
		end

		def start
			_listen_redis()
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

			# Data frame: [type, data(contains timestamp)]
			@current_data_frame = {}
			@next_data_frame = {}
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
				if type == "t\n" # Skip two lines to boost up.
					reader.readline()
					type = reader.readline()
				end
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
					if type == "odbk\n"
						@mgr.market_client(m).update_odbk(p, msg)
						changed_odbks[mp] = msg
						odbk_changed = true
					elsif type == "t\n"
						@mgr.market_client(m).update_tick(p, msg)
						changed_trades[mp] = msg
					else
						raise "Unknown type #{type.inspect}"
					end
				end
			end
			if @algo
				@algo.on_odbk(changed_odbks) if odbk_changed
				@algo.on_tick(changed_trades) if changed_trades.size > 0
			end
		end

		include URN::CLI
		def start
			if @algo.nil?
				puts "HistoryMktDataSource start without algo".red
			else
				puts "HistoryMktDataSource start with #{@algo.name}"
			end
			_prepare_his_file()
			ct = 0
			history_start_t = @current_data_frame.values.first[1].last
			start_t = Time.now.to_f
			seg_t = Time.now.to_f
			seg_n = 50_000
			loop do
				break if @finished
				if ct % seg_n == 0
					end_t = Time.now.to_f
					history_end_t = @current_data_frame.values.first[1].last
					history_span_hr = (history_end_t - history_start_t)/3600_000
					speed_h = (history_span_hr/(end_t-start_t)).round(2)
					speed_l = (seg_n/(end_t-seg_t)/1000).round
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
						stat = @algo.stat
						name = @algo.name
						filled_o = (stat[:filled_buy]||0) + (stat[:filled_sell]||0)
						total_o = (stat[:dead_buy]||0) + (stat[:dead_sell]||0)
						print [
							"#{(end_t-start_t).round}s",
						 	"#{history_span_hr.round}hrs",
							"#{speed_h}/s",
							"#{speed_l}K/s",
							name,
							"pnl:#{stat[:pnl]}",
							"SL:#{stat[:stoploss_ct]}",
							"fil:#{filled_o}/#{total_o}",
							"tk:#{stat[:taker_ct]}",
							"p:#{stat[:mkt_price]}",
							"\n"
						].join(' ')
					end
					seg_t = Time.now.to_f
				end
				ct += 1
# 				break if ct == 50 # Fast test
#  				break if ct == 5_000 # Fast test
#  				break if ct == 50_000 # Fast test
				_run_historical_files()
			end
			end_t = Time.now.to_f
			history_end_t = @current_data_frame.values.first[1].last
			history_span_hr = (history_end_t - history_start_t)/3600_000
			speed_h = (history_span_hr/(end_t-start_t)).round(2)
			puts "#{(end_t-start_t).round} s, #{ct} msg, #{history_span_hr} hrs, #{speed_h} hrs/s"
			if @algo
				stat = {:name=>@algo.name}.merge(@algo.stat())
				puts JSON.pretty_generate(stat).blue
				dir = "#{URN::ROOT}/output/#{@algo.name}/"
				FileUtils.mkdir_p dir
				puts "Flush output to #{dir}"
				@algo.write_output(dir)
				stat
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
