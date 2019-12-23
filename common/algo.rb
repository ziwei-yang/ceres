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
				@pending_orders = Concurrent::Hash.new(@pending_orders)
				@buy_orders = Concurrent::Array.new(@buy_orders)
				@dead_buy_orders = Concurrent::Array.new(@dead_buy_orders)
				@sell_orders = Concurrent::Array.new(@sell_orders)
				@dead_sell_orders = Concurrent::Array.new(@dead_sell_orders)
				@alive_stoploss_orders = Concurrent::Array.new(@alive_stoploss_orders)
			else
				@name = self.class().name().split('::').last
				@market_pairs = market_pairs
				@stat = {}

				@debug = opt[:debug] == true
				@verbose = opt[:verbose] == true
				# :live :dryrun :backtest
				@mode = opt[:mode] || :dryrun

				# Managed Orders
				@pending_orders = Concurrent::Hash.new
				@buy_orders = Concurrent::Array.new
				@dead_buy_orders = Concurrent::Array.new
				@sell_orders = Concurrent::Array.new
				@dead_sell_orders = Concurrent::Array.new
				# Stoploss orders are in buy/sell/dead orders already.
				# Use alive_stoploss_orders to mark them.
				@alive_stoploss_orders = Concurrent::Array.new
			end

			# Collect unprocessed updates from on_order_update()
			@_incoming_updates = Concurrent::Array.new

			# All instance_variables starts with underline will not be saved.
			# All _skip_save_attrs will not be saved.
			@_skip_save_attrs = [:@mgr]

			@canceling_orders ||= Concurrent::Hash.new
			@market_pairs.each { |m, p| @canceling_orders[m] ||= Concurrent::Hash.new }

			@min_valid_odbk_depth = 10 # Orderbook less than this depth will be treat as invalid.

			@_should_print_info = Concurrent::Array.new
			@_should_save_state = Concurrent::Array.new
			@_helper_thread = Thread.new {
				loop {
					if @_should_save_state.delete_at(0) != nil
						@_should_save_state.clear
						save_state_sync()
					end
					if @_should_print_info.delete_at(0) != nil
						@_should_print_info.clear
						print_info_sync()
					end
					sleep 0.1
				}
			}
			@_helper_thread.priority = -3
		end

		def _odbk_valid?(odbk)
			bids, asks, t, mkt_t = odbk
			if bids.nil? || asks.nil? || bids.size < @min_valid_odbk_depth || asks.size < @min_valid_odbk_depth
				_stat_inc(:odbk_invalid)
				return false
			end
			return true
		end

		# Final setup and first run once connected to manager
		def mgr=(mgr) # Ready to connect to DataSource
			@mgr = mgr
			mgr.add_listener(self)
			prepare() unless @_prepared == true
		end

		def prepare
			# Force redirect all log in live mode.
			# Logger would only allow one algo running to direct logs.
			if @mode == :live
				APD::Logger.global_output_file = "#{URN::ROOT}/logs/#{@name}.#{@mode}.log"
			end
			[@buy_orders, @sell_orders].each do |orders|
				orders.each { |o| @mgr.monitor_order(o) }
			end
			organize_orders()
			@_prepared = true
		end

		def print_info_sync
			# To avoid interrupted by other thread,
			# Build logs to print them at once.
			logs = ["Unconfirmed orders:"]
			logs.concat(@pending_orders.values.map { |o| "#{o['client_oid']}\n#{format_trade(o)}" })
			logs.push("Open orders:")
			logs.concat(@buy_orders.map { |o|
				if @canceling_orders[o['market']][o['i']].nil?
					next format_trade(o)
				else
					next format_trade(o).on_light_black
				end
			})
			logs.concat(@sell_orders.map { |o|
				if @canceling_orders[o['market']][o['i']].nil?
					next format_trade(o)
				else
					next format_trade(o).on_light_black
				end
			})
			logs.push("Dead orders:")
			his_logs = (@dead_buy_orders+@dead_sell_orders).
				select { |o| (o['executed'] || o['executed_v'] || 0) > 0 }.
				sort_by { |o| o['t'] }.
				reverse[0..9].
				map { |o| format_trade(o) }
			logs.concat(his_logs)
			puts "#{logs.join("\n")}\n@stat: #{@stat.to_json}"
		end

		def print_info
			@_should_print_info.push(true)
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
					if v.is_a?(Array)
						puts "Recover #{k} => #{v.size} size array"
					elsif v.is_a?(Hash)
						puts "Recover #{k} => #{v.size} size hash"
					else
						puts "Recover #{k} => #{v.inspect[0..49]}"
					end
					instance_variable_set(k.to_sym, v)
				else
					raise "Unknown content key #{k} #{v}"
				end
			end
			@_load_from_file = true
		end

		def save_state_async
			return if @mode == :backtest
			@_should_save_state.push(true)
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
			@_incoming_updates.push(trade)
			if process_order_updates()
				organize_orders()
				_core_algo(@_latest_odbk, repeat:"on_order_update() #{trade['i']}")
			end
		end
		def process_order_updates() # Just like query_orders() in local cache.
			return false if @_incoming_updates.empty?
			updated = false
			loop do # Keep applying updates (from old to latest)
				new_o = @_incoming_updates.delete_at(0)
				break if new_o.nil? # Out of data.
				found = false
				(new_o['T'] == 'buy' ? @buy_orders : @sell_orders).each do |o|
					next unless order_same?(o, new_o)
					found = true
					# Replace if needed
					if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
						puts "Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
						new_o.each { |k, v| o[k] = v }
						if order_alive?(new_o) == false # Clear canceling flag if exist.
							@canceling_orders[new_o['market']].delete(new_o['i'])
						end
						updated = true
					end
					break
				end
				next if found
				# Very few updates belongs to known dead orders.
				# They are duplicated canceled notifications.
				(new_o['T'] == 'buy' ? @dead_buy_orders : @dead_sell_orders).each do |o|
					next unless order_same?(o, new_o)
					found = true
					# Replace if needed
					if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
						puts "Dead Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
						new_o.each { |k, v| o[k] = v }
						updated = true
					end
					break
				end
				next if found
				# No correspond order found
				raise "Unexpected order updates:\n#{format_trade(new_o)}"
			end
			# In backtesing, orders attributes are always updated in DummyMarketClient
			# So order_should_update() always returns false.
			return true if @mode == :backtest || updated
			return updated
		end

		# Classify orders by state if process_order_updates() returns true.
		def organize_orders
			new_filled_orders = []
			open_buy_pos = 0
			open_sell_pos = 0

			@buy_orders.delete_if { |o|
				if order_alive?(o)
					open_buy_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				@dead_buy_orders.push o # Order is dead.
				next true if o[@exec_k] == 0
				_stat_inc(:filled_buy)
				new_filled_orders.push o
				next true
			}

			@sell_orders.delete_if { |o|
				if order_alive?(o)
					open_sell_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				@dead_sell_orders.push o # Order is dead.
				next true if o[@exec_k] == 0
				_stat_inc(:filled_sell)
				new_filled_orders.push o
				next true
			}

			pos_changed = !(new_filled_orders.empty?)
			@_open_buy_pos = open_buy_pos
			@_open_sell_pos = 0-open_sell_pos

			# Any stoploss_order filled? Maintain alive_stoploss_orders
			# Only the last one could be active order.
			# Others should be old stoploss orders which has not been fully canceled.
			latest_stoploss_order = @alive_stoploss_orders.last
			@alive_stoploss_orders.delete_if { |o|
				next false if order_alive?(o)
				if order_same?(o, latest_stoploss_order)
					puts "Stoploss order filled" if @verbose
					_stat_inc(:stoploss_ct)
				end
				if o[@exec_k] > 0 # History order is filled....
					# This would take effect in on_new_filled_orders() for new pnl and pos.
					puts "oops, history stoploss order is filled" if @verbose
					_stat_inc(:his_stoploss_ct)
				end
				next true
			}

			on_new_filled_orders(new_filled_orders)
			return pos_changed
		end

		###############################################
		# Event handlers
		###############################################
		def on_new_filled_orders(new_filled_orders)
			# Should be overwritten by sub-class
		end

		def on_place_order_done(client_oid, trade)
			puts "on_place_order_done #{client_oid} -> #{trade['i']}"
			order = @pending_orders.delete(client_oid)
			raise "@pending_orders has no such record" if order.nil?
			(trade['T'] == 'buy' ? @buy_orders : @sell_orders).push(trade)
			print_info() if @verbose
			@mgr.monitor_order(trade)
			_core_algo(@_latest_odbk, repeat:"on_place_order_done #{client_oid}")
		end

		def on_place_order_rejected(client_oid, e=nil)
			puts "on_place_order_rejected #{client_oid}"
			APD::Logger.error e unless e.nil?
			@pending_orders.delete(client_oid)
			print_info() if @verbose
			_core_algo(@_latest_odbk, repeat:"on_place_order_rejected #{client_oid}")
		end

		###############################################
		# Place/cancel orders
		###############################################
		# Callback: on_place_order_done/on_place_order_rejected
		def place_order_async(order, opt)
			opt[:allow_fail] = true # Force allowing fail.
			client_oid = @mgr.place_order_async(order, self, @pending_orders, opt)
		end
		# Callback: on_order_update()
		def cancel_order_async(orders)
			if orders.is_a?(Array)
				orders.each { |o| @canceling_orders[o['market']][o['i']] = o }
			else
				@canceling_orders[orders['market']][orders['i']] = orders
			end
			@mgr.cancel_order_async(orders)
		end

		###############################################
		# Stat printing
		###############################################
		def write_output(dir)
			puts "Nothing to be flush into #{dir}"
		end
	end
end
