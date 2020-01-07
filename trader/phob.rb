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

		# Must overwrite methods.
		# [{[mkt, pair] => [bids, asks, t]}]
		def _process_odbk_updates(changed_odbk_list)
			changed_odbk_list.each { |changed_odbk|
				_stat_inc(:odbk_update_ct)
				odbk = changed_odbk[@mp]
				next if odbk.nil?
				next if _odbk_valid?(odbk) == false
				@_latest_odbk = odbk
			}
		end
		def _process_tick_updates(ticks);end

		def work
			puts ['work() with debug: @_latest_odbk.nil?', @_latest_odbk.nil?] if @debug
			return if @_latest_odbk.nil?
			bids, asks, t, mkt_t = @_latest_odbk
			t = mkt_t unless mkt_t.nil?

			bid_top_p, ask_top_p = _stat_odbk(bids, asks)

			bid_price = bid_top_p - 19*@price_tick
			ask_price = ask_top_p + 19*@price_tick
			
			puts [bid_price, bid_top_p, ask_top_p, ask_price] if @debug
			need_place_buy = true
			@buy_orders.each do |o|
				puts format_trade(o) if @debug
				if o['p'] == bid_price
					need_place_buy = false
				else
					cancel_order_async(o)
				end
			end

			need_place_sell = true
			@sell_orders.each do |o|
				puts format_trade(o) if @debug
				if o['p'] == ask_price
					need_place_sell = false
				else
					cancel_order_async(o)
				end
			end

			@pending_orders.each do |client_oid, o|
				puts format_trade(o) if @debug
				if o['T'] == 'buy'
					if need_place_buy && o['p'] == bid_price
						need_place_buy = false
					else
						cancel_pending_order_async(o)
					end
				elsif o['T'] == 'sell'
					if need_place_sell && o['p'] == ask_price
						need_place_sell = false
					else
						cancel_pending_order_async(o)
					end
				end
			end

			place_order('buy', bid_price, @maker_size) if need_place_buy
			place_order('sell', ask_price, @maker_size) if need_place_sell

			@_stat_line += [bid_price, bid_top_p, ask_top_p, ask_price]
		end

		def place_order(type, p, s)
			order = {'pair'=>@pair, 'p'=>p, @size_k=>s, 'T'=>type, 'market'=>@market}
			order = place_order_async(order, 1, tif:'PO')
		end

		# If one side top price stay unchanged, and another side is changed (backoff/advance),
		# stat another side according to price in last frame.
		def _stat_odbk(bids, asks)
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
		market_pairs = algo.market_pairs
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

	if algo.mode == :live
		10.times { |i|
			sleep 1
			puts "Algo initialized, start data source in #{10-i}s"
		}
	end
	ret = mds.start()
end
