# Base framework for single market algorithm.
module URN
	# Each MarketAlgo has a core thread, which would be waken by others
	class MarketAlgo
		include URN::CLI
		include APD::LockUtil
		attr_reader :market_pairs, :stat, :name, :verbose, :mode, :_stat_line, :mgr

		def initialize(market_pairs, opt={})
			if market_pairs.is_a?(String)
				load_state(market_pairs)
				@mode = @mode.to_sym
				@pending_orders = Concurrent::Hash.new(@pending_orders) # TODO pending_orders missed after loaded
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

			# All instance_variables starts with underline will not be saved.
			# All _skip_save_attrs will not be saved.
			@_skip_save_attrs = [:@mgr]

			@canceling_orders ||= Concurrent::Hash.new
			@market_pairs.each { |m, p| @canceling_orders[m] ||= Concurrent::Hash.new }

			@min_valid_odbk_depth = 10 # Orderbook less than this depth will be treat as invalid.

			# Received updates that needs to be processed.
			@_odbk_updates = Concurrent::Array.new
			@_tick_updates = Concurrent::Array.new
			@_order_updates = Concurrent::Array.new
			@_misc_updates = Concurrent::Array.new

			# Background tasks.
			@_should_print_info = Concurrent::Array.new
			@_should_save_state = Concurrent::Array.new
			@_helper_thread = Thread.new(abort_on_exception:true) {
				Thread.current[:name] = "#{@name}.helper_thread"
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

		# Final setup and first run once connected to manager
		def mgr=(mgr) # Ready to connect to DataSource
			@mgr = mgr
			mgr.add_listener(self)
			prepare()
		end

		def prepare
			return if @_prepared
			@_prepared = true
			# Force redirect all log in live mode.
			# Logger would only allow one algo running to direct logs.
			if @mode == :live
				APD::Logger.global_output_file = "#{URN::ROOT}/logs/#{@name}.#{@mode}.log"
			end
			[@buy_orders, @sell_orders].each do |orders|
				orders.each { |o| @mgr.monitor_order(o) }
			end
			# TODO refresh by client_oid
			# @pending_orders.each { |client_oid, o| @mgr.monitor_order(o) }
			_organize_orders()
		end

		# For live/dryrun mode, use work_thread to wait for data/trade updates
		# For backtest mode, all work() processed in main thread.
		def start
			if @mode == :live  || @mode == :dryrun
				return if @_work_thread != nil
				@verbose = true
				@_work_thread = Thread.new(abort_on_exception:true) {
					Thread.current[:name] = "#{@name}.work_thread"
					Thread.current.priority = 3
					loop {
						# During work(), new updates might come in.
						# Always do process_updates() again after work()
						if process_updates()
							work()
							if @verbose
								puts "#{@_stat_line.join(' ')} ", nohead:true, inline:true, nofile:true
							end
						else
							sleep()
						end
					}
				}
			end
		end

		# Notify work_thread to process updates.
		# OR
		# in current thread (backtest mode)
		def wakeup
			if @mode == :live  || @mode == :dryrun
				@_work_thread.wakeup if @_work_thread != nil
			elsif @mode == :backtest
				work() if process_updates()
			else
				raise "Unknown mode #{@mode}"
			end
		end

		def work
			raise "Should overwrite work()"
		end

		################################################
		# Events
		################################################
		# Invoked by data source.
		# {[mkt, pair] => [bids, asks, t]}
		def on_odbk(changed_odbk, opt={})
			@_odbk_updates.push(changed_odbk)
			@_data_stat_line = opt[:stat_line]
			wakeup()
		end
		def _process_odbk(odbk)
			raise "Should overwrite _process_odbk()"
		end

		# Invoked by data source.
		# {[mkt, pair] => latest_trades}
		def on_tick(latest_trades)
			@_tick_updates.push(latest_trades)
			@_data_stat_line = opt[:stat_line]
			wakeup()
		end
		def _process_tick(trades)
			raise "Should overwrite _process_tick()"
		end

		# Invoked by OMS updates listener, after placing order.
		def on_place_order_done(client_oid, trade)
			@_misc_updates.push([:place_order_done, client_oid, trade])
		end
		def on_place_order_rejected(client_oid, e=nil)
			@_misc_updates.push([:place_order_rejected, client_oid, e])
		end
		def _process_misc_update(data)
			if data[0] == :place_order_done
				type, client_oid, trade = data
				puts "on_place_order_done #{client_oid} -> #{trade['i']}\n#{format_trade(trade)}"
				order = @pending_orders.delete(client_oid)
				return(puts("@pending_orders has no such record")) if order.nil?
				(trade['T'] == 'buy' ? @buy_orders : @sell_orders).push(trade)
				@mgr.monitor_order(trade)
			elsif data[0] == :place_order_rejected
				type, client_oid, e = data
				puts "on_place_order_rejected #{client_oid}"
				APD::Logger.error e unless e.nil?
				@pending_orders.delete(client_oid)
			else
				raise "Unknown misc update #{data}"
			end
		end

		# This would be invoked in background thread
		# And should return as fast as possible.
		def on_order_update(trade, opt={})
			@_order_updates.push(trade)
			wakeup()
		end
		def _process_order(new_o)
			return if @mode == :backtest # Orders are always updated
			# TODO pending orders
			found = false
			updated = false
			(new_o['T'] == 'buy' ? @buy_orders : @sell_orders).each do |o|
				next unless order_same?(o, new_o)
				found = true
				# Replace if needed
				if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
					update = true
					puts "Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
					new_o.each { |k, v| o[k] = v }
					if order_alive?(new_o) == false # Clear canceling flag if exist.
						@canceling_orders[new_o['market']].delete(new_o['i'])
					end
				end
				break
			end
			return if found
			# Very few updates belongs to known dead orders.
			# They are duplicated canceled notifications.
			(new_o['T'] == 'buy' ? @dead_buy_orders : @dead_sell_orders).each do |o|
				next unless order_same?(o, new_o)
				found = true
				# Replace if needed
				if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
					update = true
					puts "Dead Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
					new_o.each { |k, v| o[k] = v }
				end
				break
			end
			return updated
			# No correspond order found
			raise "Unexpected order updates:\n#{format_trade(new_o)}"
		end

		# Work thread : process odbk/tick/order updates before work()
		def process_updates
			return false unless @_prepared == true
			updated = false
			order_updated = false
			update_info = [0,0,0,0]
			@debug = false # Set debug=true here
			loop {
				odbk = @_odbk_updates.delete_at(0)
				break if odbk.nil?
				updated = true
				update_info[0] += 1
				_process_odbk(odbk)
			}
			loop {
				tick = @_tick_updates.delete_at(0)
				break if tick.nil?
				updated = true
				update_info[1] += 1
				_process_tick(tick)
			}
			loop {
				data = @_misc_updates.delete_at(0)
				break if data.nil?
				updated = true
				update_info[2] += 1
				_process_misc_update(data)
			}
			loop {
				trade = @_order_updates.delete_at(0)
				break if trade.nil?
				updated = true
				order_updated = true
				update_info[3] += 1
				_process_order(trade)
			}
			_organize_orders() if order_updated
			if @_data_stat_line.nil?
				@_stat_line = ['u', update_info.join(',')]
			else
				@_stat_line = @_data_stat_line + ['u', update_info.join(',')]
			end
			# Log large updates
			puts @_stat_line.join(" ").red if update_info.max > 1
			return updated
		end

		################################################
		# Utility functions.
		################################################
		def _odbk_valid?(odbk)
			bids, asks, t, mkt_t = odbk
			if bids.nil? || asks.nil? || bids.size < @min_valid_odbk_depth || asks.size < @min_valid_odbk_depth
				_stat_inc(:odbk_invalid)
				return false
			end
			return true
		end

		def _stat_inc(key, value=1)
			@stat[key] ||= 0
			@stat[key] += value
		end

		# Classify orders by state if process_order_updates() returns true.
		def _organize_orders
			new_filled_orders = []
			open_buy_pos = 0
			open_sell_pos = 0

			@buy_orders.delete_if { |o|
				puts [format_trade(o), order_alive?(o)].join if @debug
				if order_alive?(o)
					open_buy_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				next true if o[@exec_k] == 0
				@dead_buy_orders.push o # Order is dead.
				_stat_inc(:filled_buy)
				new_filled_orders.push o
				next true
			}

			@sell_orders.delete_if { |o|
				puts [format_trade(o), order_alive?(o)].join if @debug
				if order_alive?(o)
					open_sell_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				next true if o[@exec_k] == 0
				@dead_sell_orders.push o # Order is dead.
				_stat_inc(:filled_sell)
				new_filled_orders.push o
				next true
			}

			# Only keep latest filled orders, large states would slow down every thread in same process!
			# TODO this would affect pnl

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

			print_info_async() if @verbose
			on_new_filled_orders(new_filled_orders)
			return pos_changed
		end
		def on_new_filled_orders(new_filled_orders)
			if @debug && new_filled_orders.size > 0
				puts "New filled orders:"
				new_filled_orders.each { |o| puts format_trade(o) }
			end
			# Could be overwritten by sub-class
		end

		###############################################
		# Place/cancel orders
		###############################################
		# Callback: on_place_order_done/on_place_order_rejected
		# Max same price & type order in pending order is 1.
		def place_order_async(order, max_same_pending=1, opt)
			dup_ct = 0
			@pending_orders.each do |client_oid, o|
				dup_ct += 1 if o['T'] == order['T'] && o['p'] == order['p']
			end
			if dup_ct >= max_same_pending
				puts "place order intention rejected, max_same_pending #{max_same_pending} reached"
				return
			end

			old_priority = Thread.current.priority
			Thread.current.priority = 3
			opt[:allow_fail] = true # Force allowing fail.
			client_oid = @mgr.place_order_async(order, @pending_orders, opt)
			Thread.current.priority = old_priority
			# Pending orders should contains client_oid now
			save_state_async()
		end
		thread_safe :place_order_async # In case of duplicated @pending_orders

		# Callback: on_order_update()
		def cancel_order_async(orders)
			old_priority = Thread.current.priority
			Thread.current.priority = 3
			if orders.is_a?(Array)
				orders.each { |o| @canceling_orders[o['market']][o['i']] = o }
			else
				@canceling_orders[orders['market']][orders['i']] = orders
			end
			@mgr.cancel_order_async(orders)
			Thread.current.priority = old_priority
		end

		###############################################
		# Generate output file
		###############################################
		def write_output(dir)
			puts "Nothing to be flush into #{dir}"
		end

		################################################
		# Background tasks - info printing, state saving
		################################################
		def print_info_async
			if @debug
				print_info_sync()
			else
				@_should_print_info.push(true)
			end
		end
		def print_info_sync
			# To avoid interrupted by other thread,
			# Build logs to print them at once.
			logs = ["Unconfirmed orders:"]
			logs = ["DEBUG MODE - Unconfirmed orders:".light_blue.on_light_black] if @debug
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

			logs.push("Threads:")
			Thread.list.sort_by { |thr| thr.priority }.reverse.each { |thr|
				if thr == Thread.main
					logs.push("#{thr.priority.to_s.rjust(2)} #{thr[:name] || thr} [#{thr.status}] <- MAIN")
				else
					logs.push("#{thr.priority.to_s.rjust(2)} #{thr[:name] || thr} [#{thr.status}]")
				end
			}
			if @debug # print right now!
				print "#{logs.join("\n")}\n@stat: #{@stat.to_json}\n"
			else
				puts "#{logs.join("\n")}\n@stat: #{@stat.to_json}"
			end
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
				puts "save_state_sync() started"
				save_t = Time.now.to_f
				save_json = {}
				instance_variables().each do |k|
					next if k.nil?
					next if k.to_s.start_with?('@_')
					next if @_skip_save_attrs.include?(k)
					save_json[k] = instance_variable_get(k)
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
				save_t = (Time.now.to_f - save_t).round(3)
				puts "#{gzfilename} #{content.size} B in #{save_t} S"
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
	end
end
