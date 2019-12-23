require_relative '../common/bootstrap'

module URN
	# A silly market maker always maintains ask20-bid20 orders.
	# For testing full async order operations under heavy load.
	class PhobosOdbkAlgo < URN::MarketAlgo
		include URN::OrderUtil

		def initialize(market_pairs, opt={})
			super(market_pairs, opt)

			# Fixed model scale
			@maker_size = 10000.0
			@maker_size = 1.0 if @mode == :live

			raise "Only support one market." if @market_pairs.keys.size != 1
			@market = @market_pairs.keys.first
			@pair = @market_pairs.values.first
			@mp = [@market, @pair]

			case @market
			when 'Bybit'
				@maker_fee = -0.025 / 100.0
				@taker_fee = 0.075 / 100.0
				@price_tick = 0.5
				@size_k = 'v' # 's' for asset based market.
				@vol_based = (@size_k == 'v')
				@exec_k = 'executed_v' # 's' for asset based market.
			else
				raise "Unknown market #{@market}"
			end

			puts "#{self.class} is ready, #{@market_pairs} #{@name}"
		end

		def on_tick(latest_trades)
		end

		# Stat non-latest orderbook data
		# Return latest_odbk if changed_odbk is valid data.
		def _process_odbks(changed_odbk)
			return nil unless @_prepared == true

			_stat_inc(:odbk_update_ct)
			incoming_odbks = changed_odbk[@mp]
			return nil if incoming_odbks.nil?

			# Filter valid odbk from incoming_odbks
			incoming_odbks = incoming_odbks.select { |odbk| _odbk_valid?(odbk) }
			return nil if incoming_odbks.empty?

			@_latest_odbk = incoming_odbks.last
		end

		def on_odbk(changed_odbk)
			latest_odbk = _process_odbks(changed_odbk)
			return if latest_odbk.nil?

			_core_algo(latest_odbk)

			if process_order_updates()
				organize_orders()
				_core_algo(@_latest_odbk, repeat:"process_order_updates -> true")
			end
		end

		# Would be invoked in data events: on_odbk/on_tick
		# Or in placing order events: on_place_order_done/on_place_order_rejected
		# Or after order updates: after organize_orders()
		def _core_algo(latest_odbk, opt={})
			puts "Repeat _core_algo #{opt[:repeat]}" if opt[:repeat]
			return if latest_odbk.nil?
			bids, asks, t, mkt_t = latest_odbk
			t = mkt_t unless mkt_t.nil?

			bid_top_p, ask_top_p = stat_odbk(bids, asks)

			bid_price = bid_top_p - 19*@price_tick
			ask_price = ask_top_p + 19*@price_tick
			
			need_place_buy = true
			pending_orders_list = @pending_orders.values
			(@buy_orders + pending_orders_list).each do |o|
				next unless o['T'] == 'buy'
				if o['p'] == bid_price
					need_place_buy = false
				else
					cancel_order_async(o)
				end
			end
			place_order('buy', bid_price, @maker_size) if need_place_buy

			need_place_sell = true
			(@sell_orders + pending_orders_list).each do |o|
				next unless o['T'] == 'sell'
				if o['p'] == ask_price
					need_place_sell = false
				else
					cancel_order_async(o)
				end
			end
			place_order('sell', ask_price, @maker_size) if need_place_sell

			@_stat_line = [bid_price, bid_top_p, ask_top_p, ask_price].join(' ')
			puts "Repeat _core_algo done #{opt[:repeat]}" if opt[:repeat]
		end

		def on_new_filled_orders(new_filled_orders)
			print_info() if @verbose
		end

		def place_order(type, p, s)
			order = {'pair'=>@pair, 'p'=>p, @size_k=>s, 'T'=>type, 'market'=>@market}
			order = place_order_async(order, tif:'PO')
			# Pending orders should contains client_oid now
			save_state_async()
			print_info() if @verbose
		end

		# If one side top price stay unchanged, and another side is changed (backoff/advance),
		# stat another side according to price in last frame.
		def stat_odbk(bids, asks)
			ask_top_p = asks[0]['p']
			bid_top_p = bids[0]['p']
			if @_last_ask_top_p.nil? || @_last_bid_top_p.nil?
				@_last_ask_top_p = ask_top_p
				@_last_bid_top_p = bid_top_p
			elsif @_last_bid_top_p == bid_top_p && @_last_ask_top_p != ask_top_p
				# Keep last price to compute sum if one side changes this time.
			elsif @_last_bid_top_p != bid_top_p && @_last_ask_top_p == ask_top_p
				# Keep last price to compute sum if one side changes this time.
			else # Both side are changed, reset last price.
				@_last_ask_top_p = ask_top_p
				@_last_bid_top_p = bid_top_p
			end
			# Remember for next computation
			@_last_ask_top_p = ask_top_p
			@_last_bid_top_p = bid_top_p
			[bid_top_p, ask_top_p]
		end
	end
end

if __FILE__ == $0 
	market_pairs = { 'Bybit' => 'USD-BTC@P' }
	opt = {
		:mode => :live,
		:verbose => true
	}
	algo = nil

	if ARGV.size == 1 && File.file?(ARGV[0]) # Load from file.
		algo = URN::PhobosOdbkAlgo.new(ARGV[0])
		opt = {
			:mode => algo.mode,
			:verbose => algo.verbose
		}
	else
		algo = URN::PhobosOdbkAlgo.new(market_pairs, opt)
	end

	mds = URN::MktDataSource.new(
		market_pairs,
		mode: algo.mode,
		verbose: opt[:verbose],
		debug: true
	)

	mds.drive(algo)
	ret = mds.start()
end
