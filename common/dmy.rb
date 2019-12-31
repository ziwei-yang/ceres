require_relative '../common/bootstrap' unless defined? URN::BOOTSTRAP_LOAD

# This includes dummy modules that provide same functions as in common/mkt.rb
# But all operations are fake and could be emulated based on market historical data.
module URN
	class DummyMarketClient
		include URN::OrderUtil
		attr_reader :alive_orders, :dead_orders, :last_operation_time
		def initialize(name, opt={})
			@name = name
			@buy_orders = []
			@sell_orders = []
			@dead_orders = []
			@latency_ms = 70
			@last_operation_time = 0
			@verbose = opt[:verbose] == true
			@debug = opt[:debug] == true
			@listener_mgr = opt[:mgr]
		end
		def given_name
			@name
		end
		def market_name
			@name
		end
		def preprocess_deviation_evaluate(p); end

		def place_order(pair, o, opt={})
			o['status'] = 'pending'
			o['tif'] = opt[:tif]
			@last_operation_time = o['t'] = @market_t
			o['i'] = [(Time.now.to_f * 1000_000).to_i.to_s, rand(1000).to_s].join('_')
			o['client_oid'] = "client_oid_#{o['i']}"
			if o['v']
				o['executed_v'] = 0.0
				o['remained_v'] = o['v']
				o['s'] = o['v']/o['p']
			end
			o['_first_match'] = true
			o['executed'] = 0.0
			o['remained'] = o['s']
			order_status_evaluate(o)
			if o['T'] == 'buy'
				@buy_orders.push(o)
				# Sort by price+time
				@buy_orders.sort do |o1, o2|
					next o1['p'] <=> o2['p'] if o1['p'] != o2['p']
					next o1['t'] <=> o2['t']
				end
			elsif o['T'] == 'sell'
				@sell_orders.push(o)
				# Sort by reversed_price+time
				@sell_orders.sort do |o1, o2|
					next o2['p'] <=> o1['p'] if o1['p'] != o2['p']
					next o1['t'] <=> o2['t']
				end
			else
				raise "Unknown order type #{o}"
			end
 			puts ">> Order Placed - tif #{o['tif']} buy #{@buy_orders.size} sell #{@sell_orders.size}:\n#{format_trade(o)}" if @verbose
			o
		end

		def query_order(pair, trade)
			@buy_orders.each { |o| return trade if trade['i'] == o['i'] }
			@sell_orders.each { |o| return trade if trade['i'] == o['i'] }
			@dead_orders.each { |o| return trade if trade['i'] == o['i'] }
			# Raise error when trade is not under managed.
			(@buy_orders + @sell_orders + @dead_orders).each do |o|
				print "#{format_trade(o)}\n"
			end
			raise "Trade not found #{format_trade(trade)}"
		end

		def cancel_order(pair, trade)
			raise "Not implemented, should call cancel_order_async()"
		end

		def cancel_order(pair, trade)
			trade = query_order(pair, trade)
			return trade unless trade['_cancel_init_t'].nil?
			return trade unless order_alive?(trade)

			alive_orders = @buy_orders
			alive_orders = @sell_orders if trade['T'] == 'sell'
			if alive_orders.select { |o| trade['i'] == o['i'] }.empty?
				# Raise error when trade is not under managed.
				alive_orders.each { |o| print "#{format_trade(o)}\n" }
				raise "Trade not found #{format_trade(trade)}"
			end

			# Only mark trade as canceling.
			trade['_cancel_init_t'] = @last_operation_time = @market_t
			trade['status'] = 'canceling'
 			puts "-- Order Cancele request:\n#{format_trade(trade)}" if @verbose
			trade
		end

		def _mark_order_canceled(trade, reason=nil)
			trade['status'] = 'canceled'
			order_status_evaluate(trade)
			if trade['T'] == 'buy'
				@buy_orders -= [trade]
			elsif trade['T'] == 'sell'
				@sell_orders -= [trade]
			else
				raise "Unknown trade type #{trade}"
			end
			@dead_orders.push trade
 			puts "XX Order Canceled #{reason}:\n#{format_trade(trade)}" if @verbose
			@listener_mgr.notify_order_canceled(trade)
			trade
		end

		# Load frames of time series data
		# Also update managed order status.
		def update_odbk(pair, odbk)
			bids, asks, t = odbk
			@market_t = t.to_i
			return if @buy_orders.empty? && @sell_orders.empty?
			now_ms = @market_t
			# Don't update alive orders that just placed in LATENCY ms
			optimist_chance = 0
			filled_orders = []
			(@buy_orders + @sell_orders).each do |o|
				next if pair != o['pair']

				# Process canceling orders.
				if o['status'] == 'canceling'
					cancel_ms = now_ms - o['_cancel_init_t']
					if cancel_ms >= @latency_ms
						_mark_order_canceled(o, 'as requested')
						next
					end
				end

				# Process pending orders.
				alive_ms = now_ms - o['t']
				next if alive_ms <= @latency_ms
				o['status'] = 'new'

				filled = false
				if o['T'] == 'buy'
					if optimist_chance > 0 && bids[0]['p'] <= o['p']
						filled = true if rand(100) < optimist_chance
					elsif asks[0]['p'] <= o['p']
						filled = true
					end
				elsif o['T'] == 'sell'
					if optimist_chance > 0 && asks[0]['p'] >= o['p']
						filled = true if rand(100) < optimist_chance
					elsif bids[0]['p'] >= o['p']
						filled = true
					end
				end
				if filled == false
					o['_first_match'] = false
					next
				end

				# Order could be filled.
				if o['_first_match'] == true
					if o['tif'] == 'PO' # PostOnly order failed.
						_mark_order_canceled(o, "post only #{bids[0]['p']} #{asks[0]['p']}")
						next
					else
						o['_dmy_taker'] = true
					end
				else
					o['_dmy_taker'] = false
				end

				o['status'] = 'filled'
				if o['v']
					o['executed_v'] = o['v']
					o['remained_v'] = 0.0
				end
				o['executed'] = o['s']
				o['remained'] = 0.0
				order_status_evaluate(o)
				filled_orders.push(o)
				@listener_mgr.notify_order_filled(o)
			end
			if @verbose
				filled_orders.each do |o|
					puts "   Order Filled - taker? #{o['_dmy_taker']} buy #{@buy_orders.size} sell #{@sell_orders.size}:\n#{format_trade(o)}"
				end
			end
			return if filled_orders.empty?
			@buy_orders -= filled_orders
			@sell_orders -= filled_orders
			@dead_orders += filled_orders
		end

		def update_tick(pair, trades)
		end

		def stat
			{:alive=>(@buy_orders+@sell_orders).size, :dead=>@dead_orders.size}
		end

		def print_stat(opt={})
			puts "Buy: #{@buy_orders.size}"
			@buy_orders.each { |o| print "#{format_trade(o)}\n" }
			puts "Sell: #{@sell_orders.size}"
			@sell_orders.each { |o| print "#{format_trade(o)}\n" }
			if opt[:full] == true
				puts "Dead : #{@dead_orders.size}"
				@dead_orders.each { |o| print "#{format_trade(o)}\n" }
			end
		end
	end

	class DummyAssetManager < URN::StandardMarketManager
		def initialize(opt={})
			@debug = opt[:debug] == true
			@verbose = opt[:verbose] == true
			@listeners = []
		end
		def market_client(market, opt={})
			market = market['market'] if market.is_a?(Hash) # Extract market from order.
			@trade_clients ||= []
			client = @trade_clients.select { |c| c.given_name == market }.first
			return client unless client.nil?
			puts "Create market client #{market} on the fly"
			client = DummyMarketClient.new(
				market,
				verbose:@verbose,
				debug:@debug,
				mgr:self # For receiving order updates.
			)
			@trade_clients ||= []
			@trade_clients.push(client)
			return client
		end

		def notify_order_filled(new_o)
			@listeners.each { |l| l.on_order_update(new_o) }
		end

		def notify_order_canceled(new_o)
			@listeners.each { |l| l.on_order_update(new_o) }
		end

		def balance_all(opt={})
			puts "Do nothing in balance_all()"
		end

		def refresh_order(o, opt={})
			market_client(o).query_order(o['pair'], o)
		end
		def refresh_orders(orders, opt={})
			orders.map { |o| market_client(o).query_order(o['pair'], o) }
		end

		def monitor_order(trade); end
		def cancel_order_async(trade, opt={})
			if trade.is_a?(Array)
				trade.each { |t| market_client(t).cancel_order(t['pair'], t) }
			else
				market_client(trade).cancel_order(trade['pair'], trade)
			end
		end
		def place_order_async(o, order_cache, opt={})
			new_o = market_client(o).place_order(o['pair'], o, opt)
			client_oid = new_o['client_oid']
			# Make sure to put order under managed before request is created.
			order_cache[client_oid] = new_o
			# Order is pending now.
			@listeners.each { |l| l.on_place_order_done(client_oid, new_o) }
			client_oid
		end

		def stat
			@trade_clients.map { |c| [c.market_name, c.stat] }.to_h
		end

		def print_stat
			@trade_clients.map do |c|
				puts c.market_name
				c.print_stat
			end
		end
	end
end

if __FILE__ == $0 && defined? URN::BOOTSTRAP_LOAD
	puts "Run HistoryMktDataSource"
	mds = URN::HistoryMktDataSource.new({
		'Bybit' => 'USD-BTC@P'
	}, debug:false)
	mds.start
end
