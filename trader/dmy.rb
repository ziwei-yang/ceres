require_relative '../common/bootstrap'

class DummyAlgo < URN::MarketAlgo
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

		# Internal statistic/prefetech.
		@stat = {}
		@_order_place_t = 0
		@_open_buy_pos = 0 # Waiting size
		@_open_sell_pos = 0 # Waiting size

		@max_position = @maker_size # Do simple one order model first.
		@min_valid_odbk_depth = 10 # Orderbook less than this depth will be treat as invalid.

		# Rewrite name.
		@name = "DMY_#{@market}"

		puts "#{self.class} is ready, #{@market_pairs} #{@name}"
	end

	def _process_odbk_updates(changed_odbk_list) # For live
		changed_odbk_list.each { |changed_odbk| _process_odbk_update(changed_odbk) }
	end
	def _process_odbk_update(changed_odbk) # For faster backtesting
		_stat_inc(:odbk_update_ct)
		odbk = changed_odbk[@mp]
		return if odbk.nil?
		return if _odbk_valid?(odbk) == false
		@_latest_odbk = odbk
	end

	def _process_tick_updates(ticks);end

	def work
		puts ['work() with debug: @_latest_odbk.nil?', @_latest_odbk.nil?] if @debug
		return if @_latest_odbk.nil?
		bids, asks, t, mkt_t = @_latest_odbk
		t = mkt_t unless mkt_t.nil?

		bid_top_p, ask_top_p = bids[0]['p'], asks[0]['p']
		@stat[:mkt_price] = bid_top_p

		# Buy under 7000, sell above 7200
		new_order_client_oid = nil
		if ask_top_p < 7000 && @position < @max_position
			new_order_client_oid = place_order('buy', ask_top_p, [(@max_position-@position), @maker_size].min)
			cancel_all_async('sell')
		elsif bid_top_p > 7200 && @position > -@max_position
			new_order_client_oid = place_order('sell', bid_top_p, [(@position+@max_position), @maker_size].min)
			cancel_all_async('buy')
		else
			cancel_all_async()
		end

		# Mark time, stop placing order in short time? TODO
		@_order_place_t = t if new_order_client_oid != nil
	end

	# Only keep or place one order at price, cancel others
	def place_order(type, price, size)
		rasie "Unknown type #{type}" if type != 'buy' && type != 'sell'
		is_buy = (type == 'buy')
		need_to_place = true
		(is_buy ? @buy_orders : @sell_orders).each do |o|
			puts format_trade(o) if @debug
			if need_to_place && o['p'] == price
				need_to_place = false
			else
				cancel_order_async(o)
			end
		end
		@pending_orders.each do |client_oid, o|
			puts format_trade(o) if @debug
			next unless o['T'] == type
			if need_to_place && o['p'] == price
				need_to_place = false
			else
				cancel_pending_order_async(o)
			end
		end
		return unless need_to_place
		sleep 1
		order = {'pair'=>@pair, 'p'=>price, @size_k=>size, 'T'=>type, 'market'=>@market}
		place_order_async(order, 1)
		# Post only?
		# place_order_async(order, 1, tif:'PO')
	end

	def on_new_filled_orders(new_filled_orders)
		@signal_buy_firstframe = @signal_sell_firstframe = nil # Reset timer
	end
end

# Run example backtesting
puts "#"*40
puts "BACKTEST MODE START".blue
puts "#"*40

market_pairs = { 'Bybit' => 'USD-BTC@P' }
algo = DummyAlgo.new(market_pairs, mode: :backtest)

file_args = ARGV[0] # Remained CLI args are file filters
mds = URN::HistoryMktDataSource.new(
	market_pairs,
	verbose: true,
	file_filter: file_args
)

mds.drive(algo)
ret = mds.start()

puts "#{algo.name} finished"
stat_list = ret
puts JSON.pretty_generate(stat_list[0])
