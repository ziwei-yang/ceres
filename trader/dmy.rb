require_relative '../common/bootstrap'

class DummyAlgo < URN::MarketAlgo
	def on_odbk(changed_odbk)
		_stat_inc(:odbk_ct)
	end

	def on_tick(latest_trades)
		_stat_inc(:tick_ct)
	end
end

# Run example backtesting
puts "#"*40
puts "BACKTEST MODE START".blue
puts "#"*40

market_pairs = { 'Bybit' => 'USD-BTC@P' }
algo = DummyAlgo.new(market_pairs)

file_args = ARGV[0] # Remained CLI args are file filters
mds = URN::HistoryMktDataSource.new(
	market_pairs,
	verbose: true,
	file_filter: file_args
)

mds.drive(algo)
mds.start()
