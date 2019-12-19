require_relative '../common/bootstrap' unless defined? URN::BOOTSTRAP_LOAD

module URN
	# Market data subscriber that could also drive algos or just print data.
	class MktDataSource
		include URN::MarketData
		attr_reader :algos, :asset_mgr
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
			@redis_odbk_buffer = {}
			@redis_odbk_chg = false
			@redis_tick_buffer = {}
			@redis_tick_chg = false

			# :dryrun :live
			@mode = opt[:mode] || :dryrun

			@market_client_map = {}
			@market_clients = @market_pairs.to_a.map do |mp|
				m, p = mp
				# Listen orderbook only.
				@redis_sub_channels["URANUS:#{m}:#{p}:orderbook_channel"] = mp
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

		def drive(algo_class)
			raise "@algos has been set" unless @algos.nil?
			if algo_class.is_a?(Array)
				@algos = algo_class
				@algos.each { |alg| alg.mgr = @mgr }
				puts "Will drive #{@algos.size} algos"
			elsif algo_class.is_a?(URN::MarketAlgo)
				@algos = [algo_class]
				@algos.each { |alg| alg.mgr = @mgr }
				puts "Will drive algo #{algo_class.class.name}"
			elsif algo_class.is_a?(Class)
				@algos = [algo_class.new(@market_pairs)]
				@algos[0].mgr = @mgr
				puts "Will drive #{algo_class.name} market_pairs:#{@market_pairs}"
			else
				raise "Unknown argument. #{algo_class}"
			end
		end

		def _listen_redis(work_thread)
			redis_new.subscribe(*(@redis_sub_channels.keys)) do |on|
				on.subscribe { |chn, num| puts "Subscribed to ##{chn} (#{num} subscriptions)" }
				on.message do |chn, msg| # TODO check if channle is orderbook or tick.
					m, p = mp = @redis_sub_channels[chn]
					start_t = Time.now.to_f
					# To much time cost here would lead data updates missing # sleep 5
					# Don't expect this would receive all updates, so just get snapshots.
					# refresh_orderbooks() costs 0.5~1.0 ms on localhost, 1~2ms in LAN
					# could be optimized with broadcasting data directly.
					# Either don't put refresh_orderbooks() into work_cycle(),
					# algo.on_odbk() might cost too much time that lead update lost.
					data_chg = refresh_orderbooks(
						[@market_client_map[m]],
						[p],
						@market_snapshot,
						no_real_p:true,
						cache: @market_status_cache
					)
					cost_t = (Time.now.to_f - start_t)*1000
					@_stat_line = [ 'read', cost_t.round(3).to_s.ljust(6) ]
					next unless data_chg
					buffer = @redis_odbk_buffer
					# Stack data to unprocessed history
					if buffer[mp].nil?
						buffer[mp] = [@market_snapshot[m][:orderbook]]
					else
						buffer[mp].push(@market_snapshot[m][:orderbook])
					end
					@redis_odbk_chg = true
					work_thread.wakeup
				end
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			end
		end

		################### Work cycle ####################
		def start
			# sleep/wakeup model to check need_update after long time work_cycle
			work_thread = Thread.new do
				begin
					loop do
						sleep() unless @redis_odbk_chg
						work_cycle()
						print "\r#{@spin_chars[@work_ct % @spin_chars.size]}" unless @verbose
						@work_ct += 1
					end
				rescue => e
					APD::Logger.error e
				end
			end
			work_thread.priority = 99
			_listen_redis(work_thread)
		end
		def work_cycle # call _work_cycle_int() with timing
			start_t = Time.now.to_f
			_work_cycle_int()
			now = Time.now.to_f
			cost_t = (now - start_t)*1000
			if @market_snapshot.empty?
				if @verbose
					@_stat_line += ['func', cost_t.round(3).to_s.ljust(5)]
					puts "#{@_stat_line.join(' ')}    ", nohead:true, inline:true, nofile:true
				end
			else
				bids, asks, t, mkt_t = @market_snapshot[@markets[0]][:orderbook]
				local_time_diff = now*1000 - t.to_i
				mkt_time_diff = now*1000 - mkt_t.to_i
				if @verbose
					@_stat_line += [
						'work', cost_t.round(3).to_s.ljust(5),
						'latency', local_time_diff.round(3).to_s.ljust(6), mkt_time_diff.round(3).to_s.ljust(6)
					]
					@_stat_line += (@algos || []).map { |alg| alg._stat_line }
					puts "#{@_stat_line.join(' ')}    ", nohead:true, inline:true, nofile:true
				end
			end
		end
		def _work_cycle_int
			return if @redis_odbk_chg == false
			# Data has been changed for 1 or more times.
			# Reset redis data unprocessed buffer
			unprocessed_buffer = @redis_odbk_buffer
			@redis_odbk_buffer = {}
			@redis_odbk_chg = false
			return if @algos.nil?
			@algos.each { |alg| alg.on_odbk(unprocessed_buffer) }
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
			@redis_odbk_buffer = {}
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
						@redis_odbk_buffer[mp] ||= []
						@redis_odbk_buffer[mp].push(msg)
						odbk_changed = true
					elsif type == "t\n"
						@mgr.market_client(m).update_tick(p, msg)
						changed_trades[mp] = msg
					else
						raise "Unknown type #{type.inspect}"
					end
				end
			end
			if @algos
				@algos.each do |alg|
					alg.on_odbk(@redis_odbk_buffer) if odbk_changed
					alg.on_tick(changed_trades) if changed_trades.size > 0
				end
			end
			@redis_odbk_buffer = {}
		end

		include URN::CLI
		def start
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
					if @algos.nil? || @algos.empty?
						puts [
							"#{(end_t-start_t).round}s",
							ct.to_s,
					 		"#{history_span_hr} hrs",
						 	"#{speed_h}/s",
							"#{speed_l}K/s"
						].join(', ')
					elsif @algos.size == 1
						stat = @algos[0].stat
						name = @algos[0].name
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
							"\n"
						].join(' ')
					else
						puts "#{(end_t-start_t).round} s, #{ct}, #{history_span_hr} hrs, #{speed_h}/s"
					end
					seg_t = Time.now.to_f
				end
				ct += 1
# 				break if ct == 50 # Fast test
#  				break if ct == 5_000 # Fast test
				_run_historical_files()
			end
			end_t = Time.now.to_f
			history_end_t = @current_data_frame.values.first[1].last
			history_span_hr = (history_end_t - history_start_t)/3600_000
			speed_h = (history_span_hr/(end_t-start_t)).round(2)
			puts "#{(end_t-start_t).round} s, #{ct} msg, #{history_span_hr} hrs, #{speed_h} hrs/s"
			if @algos
				stat_list = @algos.map do |alg|
					stat = {:name=>alg.name}.merge(alg.stat())
					puts JSON.pretty_generate(stat).blue
					dir = "#{URN::ROOT}/output/#{alg.name}/"
					FileUtils.mkdir_p dir
					puts "Flush output to #{dir}"
					alg.write_output(dir)
					stat
				end
				return stat_list
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
