# Base framework for single market algorithm.
module URN
	class MarketAlgo
		include URN::CLI
		include APD::LockUtil
		attr_reader :market_pairs, :stat, :name, :verbose, :mode, :_stat_line, :mgr

		def initialize(market_pairs, opt={})
			if market_pairs.is_a?(String)
				load_state(market_pairs)
				@mode = @mode.to_sym
			else
				@name = self.class().name()
				@market_pairs = market_pairs
				@stat = {}

				@debug = opt[:debug] == true
				@verbose = opt[:verbose] == true
				# :live :dryrun :backtest
				@mode = opt[:mode] || :dryrun

				# Managed Orders: alive/dead buy/sell
				@buy_orders = []
				@dead_buy_orders = []
				@sell_orders = []
				@dead_sell_orders = []
			end

			# Collect unprocessed updates from on_order_update()
			@_incoming_orders = {}
			@market_pairs.keys.each { |m| @_incoming_orders[m] = {} }

			# All instance_variables starts with underline will not be saved.
			# All _skip_save_attrs will not be saved.
			@_skip_save_attrs = [:@mgr]
		end

		def mgr=(mgr)
			@mgr = mgr
			mgr.add_listener(self)
			prepare() unless @_prepared == true
		end

		def prepare
			@buy_orders = @mgr.query_orders(@buy_orders)
			@sell_orders = @mgr.query_orders(@sell_orders)
			(@buy_orders + @sell_orders).each do |o|
				@mgr.monitor_order(o)
			end
			@_prepared = true
		end

		def print_info
			puts "Buy orders"
			@buy_orders.each { |o| puts format_trade(o) }
			puts "Sell orders"
			@sell_orders.each { |o| puts format_trade(o) }
			puts "History orders"
			(@dead_buy_orders+@dead_sell_orders).
				select { |o| (o['executed'] || o['executed_v'] || 0) > 0 }.
				sort_by { |o| o['t'] }.
				reverse[0..9].
				each { |o| puts format_trade(o) }
			puts "@stat #{JSON.pretty_generate(@stat)}" if @stat != nil
		end

		def _stat_inc(key, value=1)
			@stat[key] ||= 0
			@stat[key] += value
		end

		def load_state(file)
			raise "File #{file} not exist" unless File.file?(file)
			content = nil
			if file.end_with?('.gz')
				Zlib::GzipReader.open(file) { |gz| content = gz.read }
			elsif file.end_with?('.json')
				content = File.read(file)
			else
				raise "Unknown file type #{file}"
			end

			puts "Load from #{file}"
			JSON.parse(content).each do |k, v|
				if k.start_with?('@')
					puts "Recover #{k} => #{v.to_s[0..49]}"
					instance_variable_set(k.to_sym, v)
				else
					raise "Unknown content key #{k} #{v}"
				end
			end
			@_load_from_file = true
		end

		def save_state_async
			return if @mode == :backtest
			Concurrent::Future.execute(executor: URN::CachedThreadPool) {
				save_state_sync()
			}
		end
		def save_state_sync
			data_dir = './trader_state/'
			FileUtils.mkdir_p data_dir
			filename = "#{data_dir}/#{@name}.json"
			gzfilename = filename + '.gz'
			tmpgzfilename = filename + '.gz.tmp'

			begin
				print " F"
				save_json = {}
				instance_variables().each do |k|
					if k.to_s.start_with?('@_')
						next
					elsif @_skip_save_attrs.include?(k)
						next
					end
					save_json[k] = instance_variable_get(k)
					# puts k.inspect
				end
				content = JSON.pretty_generate(save_json)

				if content == @_last_save_txt
					print "X#{content.size}B "
					return
				end
				Zlib::GzipWriter.open(tmpgzfilename) do |gz|
					gz.write content
				end
				@_last_save_txt = content
				FileUtils.mv(tmpgzfilename, gzfilename)
				print "W#{content.size}B \n"
				puts "#{gzfilename} saved #{content.size} B"
			rescue SystemExit, Interrupt => e
				APD::Logger.error e
				sleep 1
				retry
			rescue JSON::NestingError => e
				puts "Error occurred in saving state."
				raise e
			rescue => e
				APD::Logger.error e
				retry
			end
		end
		thread_safe :save_state_sync

		#{[mkt, pair] => [bids, asks, t]}
		def on_odbk(changed_odbk)
			raise "Should overwrite on_odbk()"
		end

		#{[mkt, pair] => latest_trades}
		def on_tick(latest_trades)
			raise "Should overwrite on_tick()"
		end

		# This would be invoked in background thread
		# And should return as fast as possible.
		def on_order_update(trade)
			last_update = @_incoming_orders[trade['market']][trade['i']]
			if last_update.nil?
				@_incoming_orders[trade['market']][trade['i']] = trade
			else
				# Trade notification might come in unsorted.
				if order_should_update?(last_update, trade)
					@_incoming_orders[trade['market']][trade['i']] = trade
				end
			end
		end
		def process_order_updates() # Just like query_orders() in local cache.
			updated = false
			@_incoming_orders.each do |mkt, updates|
				updates.each do |i, new_o| # Is new_o in any alive or dead orders?
					orders = @buy_orders + @dead_buy_orders
					orders = @sell_orders + @dead_sell_orders if new_o['T'] == 'sell'
					found = false
					# Replace if needed
					orders.each do |o|
						next unless order_same?(o, new_o)
						found = true
						@_incoming_orders[mkt].delete(i)
						if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
							puts "Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
							new_o.each { |k, v| o[k] = v }
							updated = true
						end
						break
					end
					next if found
					# No correspond order found
					raise "Unexpected order updates:\n#{format_trade(new_o)}"
				end
			end
			return updated
		end
		thread_safe :on_order_update, :process_order_updates

		def write_output(dir)
			puts "Nothing to be flush into #{dir}"
		end
	end
end
