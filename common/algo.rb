# Base framework for single market algorithm.
module URN
	# Each MarketAlgo has a core thread, which would be waken by others
	class MarketAlgo
		include URN::CLI
		include URN::OrderUtil
		include APD::LockUtil
		attr_reader :market_pairs, :stat, :name, :verbose, :mode, :_stat_line, :mgr, :position, :maker_size
		attr_accessor :_stat_output_dir

		def initialize(market_pairs, opt={})
			if market_pairs.is_a?(String)
				load_state(market_pairs)
				@mode = @mode.to_sym
				@pending_orders = Concurrent::Hash.new.merge(@pending_orders)
				@buy_orders = Concurrent::Array.new(@buy_orders)
				@sell_orders = Concurrent::Array.new(@sell_orders)

				@dead_buy_orders = Concurrent::Array.new(@dead_buy_orders)
				@dead_sell_orders = Concurrent::Array.new(@dead_sell_orders)

				@archived_buy_orders = Concurrent::Array.new(@archived_buy_orders || [])
				@archived_sell_orders = Concurrent::Array.new(@archived_sell_orders || [])
				@stoploss_orders = Concurrent::Hash.new.merge(@stoploss_orders || {})
				@position = 0 if @position.nil?
				@stoploss_rate = 1.0 if @stoploss_rate.nil? # default no stoploss
				@stat.keys.each { |k| @stat[k.to_sym] = @stat.delete(k) }
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
				@archived_buy_orders = Concurrent::Array.new
				@sell_orders = Concurrent::Array.new
				@dead_sell_orders = Concurrent::Array.new
				@archived_sell_orders = Concurrent::Array.new
				# Stoploss orders are in pending/buy/sell/dead orders already.
				# Use stoploss_orders to make them exclusive. { client_oid => order }
				# Only stores pending and alive stoploss orders.
				# Only the last one could be active order.
				# Others should be old stoploss orders which has not been fully canceled.
				@stoploss_orders = Concurrent::Hash.new
				@position = 0

				# Essential key attributes needs to be initialized in child class.
				@size_k = nil # 'v' for volume based, 's' for asset based market.
				@vol_based = nil # true/false (@size_k == 'v')
				@exec_k = nil # 'executed_v' # 'executed' for asset based market.
				@stoploss_rate = 1.0 # default no stoploss
				@_load_from_file = false
			end

			# All instance_variables starts with underline will not be saved.
			# All _skip_save_attrs will not be saved.
			@_skip_save_attrs = [:@mgr]

			@canceling_orders ||= {}
			@market_pairs.each { |m, p| # market -> Concurrent::Hash
				@canceling_orders[m] = Concurrent::Hash.new # Init canceling_orders with empty
			}

			@min_valid_odbk_depth = 10 # Orderbook less than this depth will be treat as invalid.

			# Received updates that needs to be processed.
			@_odbk_updates = Concurrent::Array.new
			@_tick_updates = Concurrent::Array.new
			@_order_updates = Concurrent::Array.new
			@_misc_updates = Concurrent::Array.new

			# Internal statistic
			@_order_place_t = 0

			# Background tasks.
			@_should_print_info = Concurrent::Array.new
			@_should_save_state = Concurrent::Array.new

			@_helper_thread = Thread.new(abort_on_exception:true) {
				next if @mode == :backtest
				next if @mode == :dryrun
				Thread.current[:name] = "#{@name}.helper_thread"
				ct = 0
				loop {
					ct += 1
					sleep 1
					# Every 3600s save state. data.rb would call save_state_sync() at last.
					if ct % 3600 == 0 && @_should_save_state.delete_at(0) != nil
						@_should_save_state.clear
						save_state_sync()
					end
					if @_should_print_info.delete_at(0) != nil
						@_should_print_info.clear
						print_info_sync()
					end
				}
			}
			@_helper_thread.priority = -3

			@_data_stat_line = []
		end

		# Final setup and first run once connected to manager
		def mgr=(mgr) # Ready to connect to DataSource
			@mgr = mgr
			mgr.add_listener(self)
			prepare()
		end

		def prepare
			return if @_prepared

			if @_load_from_file == false && @mode != :dryrun
				if state_file_exist?
					raise "state file exists, no more algo could be spawned"
				else
					puts "Save state for new spawned algo"
					save_state_async()
				end
			end

			@_prepared = true
			# Force redirect all log in live mode.
			# Logger would only allow one algo running to direct logs.
			if @mode == :live
				APD::Logger.global_output_file = "#{URN::ROOT}/logs/#{@name}.#{@mode}.log"
			end

			print_info_sync() if @mode != :backtest
			# refresh pending orders and open orders
			@mgr.query_orders(@buy_orders + @sell_orders).
				each { |o| on_order_update(o) }
			@pending_orders.each do |client_oid, o|
				begin
					new_o = @mgr.query_order(o)
					on_place_order_done(client_oid, new_o)
				rescue URN::OrderNotExist => e
					puts "Order not exist for #{client_oid}\n#{format_trade(o)}, remove from pending_orders"
					on_place_order_rejected(client_oid)
				end
			end
			process_updates()
			compute_pnl()
			print_info_sync() if @mode != :backtest

			raise "pending_orders should be empty now." unless @pending_orders.empty?

			[@buy_orders, @sell_orders].each do |orders|
				orders.each { |o| @mgr.monitor_order(o) }
			end

			# Rescue history errors.
			[
				@buy_orders,
				@sell_orders,
				@dead_buy_orders,
				@dead_sell_orders,
				@archived_buy_orders,
				@archived_sell_orders
			].each { |orders|
				orders.each { |o|
					begin
						o['fee'].to_f
					rescue => e
						puts "order fee #{o['fee']} could not be coerced into float"
						new_o = @mgr.query_order(o)
						raise e if o['fee'] == new_o['fee']
						puts "order fee #{o['fee']} recovered to #{new_o['fee']}"
						o['fee'] = new_o['fee']
						retry
					end
				}
			}
		end

		# For live/dryrun mode, use work_thread to wait for data/trade updates
		# For backtest mode, all work() processed in main thread.
		def start
			return if @mode != :live && @mode != :dryrun
			return if @_work_thread != nil
			@verbose = true
			@_work_thread = Thread.new(abort_on_exception:true) {
				Thread.current[:name] = "#{@name}.work_thread"
				Thread.current.priority = 3
				loop {
					# During work(), new updates might come in.
					# Always do process_updates() again after work()
					cycle_t = Time.now.to_f
					if process_updates()
						@_stat_line_keep_log = nil
						work()
						if @verbose
							cycle_t = ((Time.now.to_f - cycle_t)*1000).round(3)
							@_stat_line.push("W #{cycle_t}")
							if cycle_t > 1
								puts "SL-AG #{@_stat_line.join(' ')} ".yellow
							elsif @_stat_line_keep_log != nil
								if @_stat_line_keep_log == true
									puts "#{@_stat_line.join(' ')} ".light_magenta
								else # Also record reason of keeping log.
									puts "#{@_stat_line_keep_log} #{@_stat_line.join(' ')} ".light_magenta
								end
							else
								puts "#{@_stat_line.join(' ')} ".light_red, nohead:true, inline:true, nofile:true
							end
						end
					else
						sleep()
					end
				}
			}
		end

		# Notify work_thread to process updates.
		# OR
		# in current thread (backtest mode)
		def wakeup
			if @mode == :backtest
				work() if process_updates()
			elsif @mode == :live || @mode == :dryrun
				@_work_thread.wakeup if @_work_thread != nil
			else
				raise "Unknown mode #{@mode}"
			end
		end

		def work
			raise "Should overwrite work()"
		end

		# When PnL drops to very low in backtesting,
		# Return true to stop running.
		def should_end_backtest?
			# Backtesting always start with marker size 10000 USD
			# should stop when Pnl < specific BTC
			if @stat[:should_end_backtest]
				puts "PnL is too low, should end backtesting".red
				@stat[:killed] = true
				return true
			end
			return false
		end

		################################################
		# Signal Stat
		################################################
		def signal_stat_init(signal_names)
			# Prepare signal stat file.
			@_stat_output_dir ||= "#{URN::ROOT}/output/"
			FileUtils.mkdir_p(@_stat_output_dir)
			@_stat_f = "#{@_stat_output_dir}/#{@name}.#{@mode}.stat.txt.gz"
			puts "Signal stat file: #{@_stat_f}"
			@_stat_w = Zlib::GzipWriter.open("#{@_stat_f}")
			his_length_list = [10, 30, 60, 300] # in seconds
			@_stat_history_list = his_length_list.map { |l|
				# One more bucket to make sure time diff >= required
				APD::TimeSeriesBucket.new(l*1000/10, 11)
			}
			# write headline
			headline = ["market_timestamp", "price"] + his_length_list.map { |l|
				["tDif#{l}", "pDif#{l}"] + signal_names.map { |n|
					"#{n}#{l}"
				}
			}
			@_stat_w.puts(headline.join("\t"))
			@_stat_empty_seg = ['-', '-'] + signal_names.map { '-' }
		end

		def signal_stat_record(mkt_t, price, signal_names, signal_list)
			signal_stat_init(signal_names) if @_stat_w.nil?

			# Save current signal for future writing.
			@_stat_history_list.each { |his|
				his.append(mkt_t, [mkt_t, price, signal_list])
			}

			# Write saved oldest stat data and current market data.
			line = [mkt_t, price]
			all_data_missing = true
			@_stat_history_list.each { |his|
				old_stat_data = his.shift
				if old_stat_data.nil?
					line.concat(@_stat_empty_seg)
				else
					all_data_missing = false
					old_mkt_t, old_p, old_signals = old_stat_data
					time_diff = ((mkt_t-old_mkt_t)/1000).round(1) # in second
					p_diff = price - old_p
					line.push(time_diff, p_diff)
					line.concat(old_signals)
				end
			}
			@_stat_w.puts(line.join("\t")) unless all_data_missing
		end

		def signal_stat_finish
			@_stat_w.close unless @_stat_w.nil?
		end

		################################################
		# Events
		################################################
		# Invoked by data source.
		# {[mkt, pair] => [bids, asks, t]}
		def on_odbk(changed_odbk, opt={})
			if @mode == :backtest # Boost for backtest mode
				_process_odbk_update(changed_odbk)
				return work()
			end
			@_odbk_updates.push(changed_odbk)
			@_data_stat_line = opt[:stat_line] || []
			wakeup()
		end
		def _process_odbk_update(odbk)
			raise "Should overwrite _process_odbk_update() for backtesting"
		end
		def _process_odbk_updates(odbk_list)
			raise "Should overwrite _process_odbk_updates()"
		end

		# Invoked by data source.
		# {[mkt, pair] => latest_trades}
		def on_tick(latest_trades, opt={})
			if @mode == :backtest # Boost for backtest mode
				_process_tick_update(latest_trades)
				return work()
			end
			@_tick_updates.push(latest_trades)
			@_data_stat_line = opt[:stat_line] || []
			wakeup()
		end
		def _process_tick_updates(trades)
			raise "Should overwrite _process_tick()"
		end

		# Invoked by OMS updates listener, after placing order.
		def on_place_order_done(client_oid, trade)
			@_misc_updates.push([:place_order_done, client_oid, trade])
			wakeup() if @mode != :backtest
		end
		def on_place_order_rejected(client_oid, e=nil)
			@_misc_updates.push([:place_order_rejected, client_oid, e])
			wakeup() if @mode != :backtest
		end
		def _process_misc_update(data)
			if data[0] == :place_order_done
				type, client_oid, trade = data
				puts "place_order_done #{client_oid} -> #{trade['i']}\n#{format_trade(trade)}" if @verbose || @mode != :backtest
				order = @pending_orders.delete(client_oid)
				return(puts("@pending_orders has no such record")) if order.nil?
				(trade['T'] == 'buy' ? @buy_orders : @sell_orders).push(trade)
				@mgr.monitor_order(trade)
				_update_stoploss_order(trade)
			elsif data[0] == :place_order_rejected
				type, client_oid, e = data
				APD::Logger.error e unless e.nil?
				o = @pending_orders.delete(client_oid)
				stoploss_o = @stoploss_orders.delete(client_oid)
				order_class = 'unknown order'
				if stoploss_o != nil
					order_class = 'stoploss order'
				elsif o != nil
					order_class = 'pending order'
				end

				if o != nil
					puts "#{order_class} rejected #{client_oid}\n#{format_trade(o)}".on_light_yellow
				else
					puts "#{order_class} rejected #{client_oid}".on_light_yellow
				end
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
			# Clear canceling order record
			if order_alive?(new_o) == false
				canceled_o = @canceling_orders[new_o['market']].delete(new_o['i'])
				if canceled_o != nil
					if canceled_o[@exec_k] > 0 # Is this filled ?
						_stat_inc(:cancel_failed)
					else
						_stat_inc(:cancel_success)
					end
				end
			end

			_update_stoploss_order(new_o)

			found = false
			updated = false
			(new_o['T'] == 'buy' ? @buy_orders : @sell_orders).each do |o|
				next unless order_same?(o, new_o)
				found = true
				# Replace if needed
				if order_should_update?(o, new_o) # Copy all attributes: new_o -> o
					update = true
					puts "Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}" if @mode != :backtest
					new_o.each { |k, v| o[k] = v }
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
			return if found

			# Sometimes old legacy updates would be received
			puts "Unexpected order updates:\n#{format_trade(new_o)}".on_light_yellow
			return
		end

		# Work thread : process odbk/tick/order updates before work()
		def process_updates
			return false unless @_prepared == true
			updated = false
			order_updated = false
			update_info = [0,0,0,0]
			# @debug = false # Set debug=true here

			new_odbk_list = []
			loop {
				odbk = @_odbk_updates.delete_at(0)
				break if odbk.nil?
				new_odbk_list.push(odbk)
				updated = true
				update_info[0] += 1
			} # Batch processing odbk updates.
			_process_odbk_updates(new_odbk_list) if new_odbk_list[0] != nil

			new_tick_list = []
			loop {
				tick = @_tick_updates.delete_at(0)
				break if tick.nil?
				new_tick_list.push(tick)
				updated = true
				update_info[1] += 1
			} # Batch processing tick updates.
			_process_tick_updates(new_tick_list) if new_tick_list[0] != nil

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
			if @mode != :backtest
				@_stat_line = @_data_stat_line + [update_info.join(',')]
				# Log large updates
				@_stat_line_keep_log = 'UPDTE' if update_info.max > 1
			end
			return updated
		end

		################################################
		# Utility functions.
		################################################
		# Update if trade client_oid is recorded in @stoploss_orders
		def _update_stoploss_order(trade)
			stoploss_o = @stoploss_orders[trade['client_oid']]
			return if stoploss_o.nil?
			puts "Stoploss order update\n#{format_trade(stoploss_o)}\n#{format_trade(trade)}" if @mode != :backtest
			trade.each { |k, v| stoploss_o[k] = v }
		end

		# Often used in signal backtesting.
		# Example: Only record signal when top price changed.
		def _top_price_changed?(odbk)
			bids, asks, t, mkt_t = odbk
			if @_last_top_bid_p == bids[0]['p'] && @_last_top_ask_p == asks[0]['p']
				return false
			else
				@_last_top_bid_p = bids[0]['p']
				@_last_top_ask_p = asks[0]['p']
				return true
			end
		end

		def _odbk_valid?(odbk)
			bids, asks, t, mkt_t = odbk
			if bids.nil? || asks.nil? || bids.size < @min_valid_odbk_depth || asks.size < @min_valid_odbk_depth
				_stat_inc(:odbk_invalid)
				return false
			end
			return true
		end

		def _tick_valid?(tick)
			tick['T'] == 'buy' || tick['T'] == 'sell'
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

			buy_delete_ct = 0
			@buy_orders.delete_if { |o|
				# puts [format_trade(o), order_alive?(o), @exec_k, o[@exec_k]].join if @debug
				if order_alive?(o)
					open_buy_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				_stat_inc(:dead_buy_orders)
				buy_delete_ct += 1
				@dead_buy_orders.push o
				next true if o[@exec_k] == 0
				new_filled_orders.push o
				next true
			}

			sell_delete_ct = 0
			@sell_orders.delete_if { |o|
				# puts [format_trade(o), order_alive?(o), @exec_k, o[@exec_k]].join if @debug
				if order_alive?(o)
					open_sell_pos += (o[@size_k]-o[@exec_k])
					next false
				end
				_stat_inc(:dead_sell_orders)
				sell_delete_ct += 1
				@dead_sell_orders.push o
				next true if o[@exec_k] == 0
				new_filled_orders.push o
				next true
			}

			@_open_buy_pos = open_buy_pos
			@_open_sell_pos = 0-open_sell_pos
			if new_filled_orders.empty?
				print_info_async() if @verbose
				return
			end

			# Only keep latest orders in dead_buy_orders/dead_sell_orders
			# large states would slow down every thread in same process!
			# Archive old filled orders.
			if buy_delete_ct > 0
				size = @dead_buy_orders.size
				if size > 30
					size.times {
						o = @dead_buy_orders.delete_at(0)
						@archived_buy_orders.push(o) if o[@exec_k] > 0
					}
				end
			end
			if sell_delete_ct > 0
				size = @dead_sell_orders.size
				if size > 30
					size.times {
						o = @dead_sell_orders.delete_at(0)
						@archived_sell_orders.push(o) if o[@exec_k] > 0
					}
				end
			end

			# Any stoploss_order filled? Maintain stoploss_orders
			# Only the last one could be active order.
			# Others should be old stoploss orders which has not been fully canceled.
			@stoploss_orders.values.each { |o|
				next if order_alive?(o)
				if o[@exec_k] == 0 # Canceled successfully
					@stoploss_orders.delete(o['client_oid'])
					next
				end
				# Order is filled.
				if @latest_stoploss_order != nil && order_same?(o, @latest_stoploss_order)
					puts "Last stoploss order filled\n#{format_trade(o)}" if @verbose
					_stat_inc(:stoploss_fill_latest)
				else
					# This would affect in on_new_filled_orders() for new pnl and pos.
					puts "oops, history stoploss order is filled\n#{format_trade(o)}" if @verbose
					_stat_inc(:stoploss_fil_his)
				end
				@stoploss_orders.delete(o['client_oid'])
			}

			# Calculate position
			puts "position before filled: #{@position}" if @verbose
			new_filled_orders.each do |o|
				if o['T'] == 'buy'
					_stat_inc(:filled_buy)
					@position += o[@exec_k]
				elsif o['T'] == 'sell'
					_stat_inc(:filled_sell)
					@position -= o[@exec_k]
				end
			end

			if @verbose
				puts "New filled orders:"
				new_filled_orders.each { |o| puts format_trade(o), nohead:true }
				puts "position after filled: #{@position}"
			end

			on_new_filled_orders(new_filled_orders)
			compute_pnl()
			print_info_async() if @verbose
			get_input prompt:"Press enter to continue" if @debug
		end

		# Could be overwritten by sub-class
		def on_new_filled_orders(new_filled_orders)
		end

		# Calculate realized PnL
		# leave last orders for current position as unrealized PnL
		def compute_pnl
			last_pos = @position
			unrealized_cost = 0
			fee = 0
			taker_ct = 0
			buy_pos, buy_pos_cost = 0, 0
			unclosed_position_t = nil
			# From latest to oldest.
			(@archived_buy_orders + @dead_buy_orders).reverse.each { |o|
				p, s = o['p'], o[@exec_k]
				next if s == 0
				cost = (@vol_based ? (s/p.to_f) : (s*p.to_f))
				size = 0 # Size for realized PNL
				if last_pos > 0 # Unprocessed unrealized position
					unclosed_position_t ||= o['t']
					if last_pos > s # Order is in last pos.
						last_pos -= s
						unrealized_cost += cost
						next
					else # Order contains part of last pos.
						size = s - last_pos
						part_unrealized_cost = (@vol_based ? (last_pos/p.to_f) : (last_pos*p.to_f))
						unrealized_cost += part_unrealized_cost
						cost -= part_unrealized_cost
						last_pos = 0
					end
				else # No unrealized position left, all as realized position
					size = s
				end
				buy_pos += size
				buy_pos_cost += cost
				if @mode == :backtest
					if o['_dmy_taker'] == true
						fee += cost*@taker_fee
						taker_ct += 1
					elsif o['_dmy_taker'] == false
						fee += cost*@maker_fee
					else
						raise "Unknown o['_dmy_taker'] #{o['_dmy_taker']}"
					end
				else
					raise "No fee in #{o.to_json}" if o['fee'].nil?
					if o['fee'].is_a?(String)
						fee += o['fee'].to_f
					else
						fee += o['fee']
					end
				end
			}
			sell_pos, sell_pos_cost = 0, 0
			# From latest to oldest
			(@archived_sell_orders + @dead_sell_orders).reverse.each do |o|
				p, s = o['p'], o[@exec_k]
				next if s == 0
				cost = (@vol_based ? (s/p.to_f) : (s*p.to_f))
				size = 0 # Size for realized PNL
				if last_pos < 0 # Unprocessed unrealized position
					unclosed_position_t ||= o['t']
					if last_pos < 0-s # Order is in last pos.
						last_pos += s
						unrealized_cost += cost
						next
					else # Order contains part of last pos.
						size = s + last_pos
						part_unrealized_cost = (@vol_based ? (last_pos.abs/p.to_f) : (last_pos.abs*p.to_f))
						unrealized_cost += part_unrealized_cost
						cost -= part_unrealized_cost
						last_pos = 0
					end
				else # No unrealized position left, all as realized position
					size = s
				end
				sell_pos += size
				sell_pos_cost += cost
				if @mode == :backtest
					if o['_dmy_taker'] == true
						fee += cost*@taker_fee
						taker_ct += 1
					elsif o['_dmy_taker'] == false
						fee += cost*@maker_fee
					else
						raise "Unknown o['_dmy_taker'] #{o['_dmy_taker']}"
					end
				else
					raise "No fee in #{o.to_json}" if o['fee'].nil?
					if o['fee'].is_a?(String)
						fee += o['fee'].to_f
					else
						fee += o['fee']
					end
				end
			end
			puts ['buy_pos', buy_pos, 'cost', buy_pos_cost] if @verbose
			puts ['sell_pos', sell_pos, 'cost', sell_pos_cost] if @verbose
			if @position == 0
				@avg_price = nil
				@position_t_ms = nil
			elsif @vol_based
				@avg_price = (@position/unrealized_cost).abs
				@position_t_ms = unclosed_position_t
			else
				@avg_price = (unrealized_cost/@position).abs
				@position_t_ms = unclosed_position_t
			end
			if @position == 0
				@stoploss_price = nil
			elsif @position > 0 # Long pos
				@stoploss_price = @avg_price*(1-(@stoploss_rate/(@position.abs/@maker_size)))
				# @stoploss_price = @avg_price*(1-@stoploss_rate)
			else # Short position
				@stoploss_price = @avg_price*(1+(@stoploss_rate/(@position.abs/@maker_size)))
				# @stoploss_price = @avg_price*(1+@stoploss_rate)
			end
			puts [
				'pos', @position,
				'cost', unrealized_cost.round(4),
				'avg_p', @avg_price,
				'stplos', @stoploss_price
			] if @verbose
			if @vol_based
				pnl = buy_pos_cost - sell_pos_cost - fee
			else
				pnl = sell_pos_cost - buy_pos_cost - fee
			end
			puts ['fee', fee.round(8), 'pnl', pnl.round(12)] if @verbose
			@stat[:fee] = fee
			@stat[:taker_ct] = taker_ct
			@stat[:pnl] = pnl.round(12)
			# see -> def should_end_backtest?
			@stat[:should_end_backtest] = true if @mode == :backtest && pnl < -0.3
			@stat[:pos] = @position
			@stat[:dead_buy_orders] = @dead_buy_orders.size
			@stat[:dead_sell_orders] = @dead_sell_orders.size
			@stat[:archived_buy_orders] = @archived_buy_orders.size
			@stat[:archived_sell_orders] = @archived_sell_orders.size
			print_info_async() if @mode == :live
		end

		###############################################
		# Place/cancel orders
		###############################################
		# Callback: on_place_order_done/on_place_order_rejected
		# Max same price & type order in pending order is 1.
		# Return client_oid if order would be placed
		def place_order_async(order, max_same_pending=1, opt={})
			reason = opt.delete(:reason)
			if @mode == :dryrun
				return
			elsif @mode == :backtest
				raise "No reason to place order" if reason.nil?
			end
			if order[@size_k] <= 0
				puts "Invalid order #{order}".red
				return nil
			end
			dup_ct = 0
			@pending_orders.each do |client_oid, o|
				dup_ct += 1 if o['T'] == order['T'] && o['p'] == order['p']
			end
			if dup_ct >= max_same_pending
				puts "place order intention rejected, max_same_pending #{max_same_pending} reached"
				return nil
			end
			old_priority = Thread.current.priority
			Thread.current.priority = 3
			opt[:allow_fail] = true # Force allowing fail.
			if @verbose
				str = "Placing order #{@pending_orders} reason:#{reason}".blue
				puts "#{str}\n#{@_reason_info}\n#{format_trade(order)}"
			end
			client_oid = @mgr.place_order_async(order, @pending_orders, opt)
			Thread.current.priority = old_priority
			# Pending orders should contains client_oid now
			save_state_async()
			@_order_place_t = Time.now.to_f
			@_stat_line_keep_log = 'PLACE'
			return client_oid
		end
		thread_safe :place_order_async # In case of duplicated @pending_orders

		# Callback: on_order_update()
		def cancel_order_async(orders)
			old_priority = Thread.current.priority
			Thread.current.priority = 3
			if orders.is_a?(Array)
				return if orders.empty?
				orders.each { |o| @canceling_orders[o['market']][o['i']] = o }
			else
				@canceling_orders[orders['market']][orders['i']] = orders
			end
			@_stat_line_keep_log = 'CANCL'
			@mgr.cancel_order_async(orders)
			Thread.current.priority = old_priority
		end

		# TODO
		def cancel_pending_order_async(order)
		end

		def cancel_all_async(type=nil)
			if type == 'buy'
				cancel_order_async(@buy_orders)
				return if @pending_orders.empty?
				@pending_orders.each { |client_oid, o|
					cancel_order_async(o) if o['T'] == type
				}
			elsif type == 'sell'
				cancel_order_async(@sell_orders)
				return if @pending_orders.empty?
				@pending_orders.each { |client_oid, o|
					cancel_order_async(o) if o['T'] == type
				}
			elsif type.nil?
				cancel_order_async(@buy_orders)
				cancel_order_async(@sell_orders)
				cancel_order_async(@pending_orders.values)
			else
				raise "Unknown type #{type}"
			end
		end

		# Place stoploss order if no duplicated order.
		# Also cancel all others.
		# Return client_oid if order would be placed.
		def place_stoploss_order_async(order, opt={})
			dup = false # Check if stoploss order is existed?
			@stoploss_orders.each do |client_oid, o|
				if o['p'] == order['p'] && o['T'] == order['T']
					dup = true
				else
					cancel_order_async(o)
				end
			end
			return nil if dup == true
			@_stat_line_keep_log = 'STPLS'
			puts "Placing stoploss order\n#{format_trade(order)}".on_light_yellow if @verbose
			opt[:reason] = "stoploss"
			client_oid = place_order_async(order, 1, opt)
			return nil if client_oid.nil?
			order = @pending_orders[client_oid]
			if order.nil?
				print_info_sync()
				puts "Order #{client_oid} is not in @pending_orders"
				raise "Order #{client_oid} is not in @pending_orders"
			end
			@latest_stoploss_order = @stoploss_orders[client_oid] = order
			puts "New stoploss order added #{client_oid}" if @verbose || (@mode != :backtest)
			client_oid
		end

		###############################################
		# Generate output file
		###############################################
		def finish
			signal_stat_finish()
		end

		################################################
		# Background tasks - info printing, state saving
		################################################
		def print_info_async
			if @debug || @mode == :backtest
				print_info_sync() # Right now!
			else
				@_should_print_info.push(true)
			end
		end
		def print_info_sync
			return if @mode == :dryrun
			print_t = Time.now.to_f
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

			if @stoploss_orders.empty? == false
				logs.push("Stoploss orders:".red)
				logs.concat(@stoploss_orders.map { |client_oid, o| format_trade(o) })
			end

			logs.push("Dead orders:")
			orders = (@dead_buy_orders+@dead_sell_orders)
			if @archived_buy_orders.size > 10
				orders += @archived_buy_orders[-10..-1]
			else
				orders += @archived_buy_orders
			end
			if @archived_sell_orders.size > 10
				orders += @archived_sell_orders[-10..-1]
			else
				orders += @archived_sell_orders
			end
			his_logs = orders.
				select { |o| (o[@exec_k] || 0) > 0 }.
				sort_by { |o| o['t'] }.
				reverse[0..9].
				map { |o| format_trade(o) }
			logs.concat(his_logs)

			logs.push("Archive orders: #{@archived_buy_orders.size} #{@archived_sell_orders.size}")

			logs.push("Threads:")
			Thread.list.sort_by { |thr| thr.priority }.reverse.each { |thr|
				str = "#{thr.priority.to_s.rjust(2)} #{thr[:name] || thr} [#{thr.status}]"
				str += " <- MAIN" if thr == Thread.main
				logs.push(str)
			}
			print_t = ((Time.now.to_f - print_t)*1000).round(3)
			puts "#{logs.join("\n")}\n@stat: #{@stat.to_json}\nin #{print_t} ms", level:2
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

		def state_file_exist?
			return false if @mode == :backtest
			data_dir = './trader_state/'
			File.file?("#{data_dir}/#{@name}.json") || File.file?("#{data_dir}/#{@name}.json.gz")
		end
		def save_state_async
			return if @mode == :backtest
			return if @mode == :dryrun
			@_should_save_state.push(true)
		end
		def save_state_sync
			return if @mode == :backtest
			return if @mode == :dryrun
			data_dir = './trader_state/'
			FileUtils.mkdir_p data_dir
			filename = "#{data_dir}/#{@name}.json"
			gzfilename = filename + '.gz'
			tmpgzfilename = filename + '.gz.tmp'

			begin
				print " F"
				puts "save_state started"
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
				save_t = ((Time.now.to_f - save_t)*1000).round(3)
				puts "#{gzfilename} #{content.size} B in #{save_t} ms"
			rescue SystemExit, Interrupt => e
				APD::Logger.error e
				sleep 1
				retry
			rescue JSON::NestingError => e
				puts "Error occurred in saving state."
				raise e
			rescue => e
				APD::Logger.error e
				sleep 1
				retry
			end
		end
	end

	module AlgoCLILoader
		class << self
			include Parallel::ProcessorCount
		end
		def self.boot(algo_class, cli_args, market_pairs=nil)
			return batch_test(algo_class, cli_args, market_pairs) if cli_args.include?('batch')

			algo, opt_algo = nil, nil
			if cli_args.size == 1 && File.file?(cli_args[0]) # Load from file.
				algo = algo_class.new(cli_args[0])
				market_pairs = algo.market_pairs
				opt_algo = {
					:mode => algo.mode,
					:verbose => algo.verbose
				}
			else
				# Parse common options first.
				cli_args, opt_common = parse_options(cli_args)
				cli_args, opt_algo = algo_class.parse_cli_opt(cli_args)
				opt_algo = opt_common.merge(opt_algo)
				opt_algo[:mode] ||= :backtest # Default mode.

				puts ('#'*50)
				if opt_algo[:mode] == :backtest
					puts "BACKTESTING MODE START"
				elsif opt_algo[:mode] == :live
					puts "LIVE MODE START".red
				elsif opt_algo[:mode] == :dryrun
					puts "DRYRUN MODE START".blue
				else
					raise "Unknown mode #{opt_algo}"
				end
				puts ('#'*50)
				puts opt_algo
				algo = algo_class.new(market_pairs, opt_algo)
			end

			# Market data source setup.
			mds = nil
			if opt_algo[:mode] == :backtest
				file_args = cli_args[0] # Remained CLI args are file filters
				# Filter start_time string
				start_time = cli_args.select { |s| s =~ /^[0-9]{8,14}$/ }.first
				if start_time != nil
					cli_args -= [start_time]
					start_time = DateTime.parse(start_time.ljust(14, '0') + " +0800")
					puts "Start from time #{start_time}"
					start_time = start_time.strftime('%Q').to_i
					puts "Start from time #{start_time}"
				end
				mds = URN::HistoryMktDataSource.new(
					market_pairs,
					verbose: opt_algo[:verbose],
					file_filter: file_args,
					start_time: start_time
				)
				algo._stat_output_dir = "#{URN::ROOT}/output/#{mds.filename()}"
			else
				mds = URN::MktDataSource.new(
					market_pairs,
					mode: algo.mode,
					verbose: opt_algo[:verbose],
					debug: true
				)
			end

			mds.drive(algo)
			if algo.mode == :live
				10.times { |i|
					sleep 1
					puts "Algo initialized, start data source in #{10-i}s"
				}
			end

			ret = mds.start()

			puts "#{algo.name} finished"
		end

		# Parse algo common options:
		# @debug/@verbose
		# @mode = :dryrun/:backtest/:live
		def self.parse_options(cli_args)
			opt = {}
			# Parse switches.
			['debug', 'verbose'].each do |sw|
				if cli_args.include?(sw)
					opt[sw.to_sym] = true
					cli_args -= [sw]
				end
			end

			# Parse run mode.
			['dryrun', 'backtest', 'live'].each do |m|
				if cli_args.include?(m)
					cli_args -= [m]
					opt[:mode] = m.to_sym
				end
			end
			return [cli_args, opt]
		end

		def self.batch_test(algo_class, option_list, cli_args, market_pairs=nil)
			processors = processor_count()/2-1 # Keep one core
			puts "Run #{algo_class} Bot in batch mode"
			algos = option_list.map { |opt| algo_class.new(market_pairs, opt) }
			cli_args -= ['batch']
			file_args = cli_args[0] # Remained CLI args are file filters
			stat_map = Parallel.map(algos, in_processes:processors) do |alg|
				mds = URN::HistoryMktDataSource.new(
					market_pairs,
					file_filter: file_args
				)
				alg._stat_output_dir = "#{URN::ROOT}/output/#{mds.filename()}"
				mds.drive(alg)
				stat_list = mds.start()
				puts "#{alg.name} finished"
				[alg.name, stat_list[0]]
			end.to_h
			return stat_map
		end
	end
end
