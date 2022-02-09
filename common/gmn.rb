require_relative '../common/bootstrap'

# Gemini client api.
# Supports allow_fail in trade functions.
module URN
	class Gemini < TradeClientBase; end
	TradeClient['Gemini'] = Gemini
	class Gemini
		include APD::ExpireResult
		def oms_enabled?
			true
		end

		def is_transferable?
			true
		end

		def deposit_fee(asset, opt={})
			0
		end

		def can_deposit?(asset)
			all_pairs(allow_fail: true) if @gemini_pair_info.nil?
			@gemini_pair_info.keys.select { |p| p.split('-').include?(asset.upcase.strip) }.size > 0
		end

		def can_withdraw?(asset)
			can_deposit?(asset)
		end

		def withdraw_fee(asset, opt={})
			# https://www.gemini.com/fees/transfer-fee-schedule#section-withdrawal-fees-individual-customers
			case asset.upcase
			when 'ZRX'
				return 1
			when 'AAVE'
				return 0.01
			when 'AMP'
				return 10
			when 'BAL'
				return 0.01
			when 'BAT'
				return 1
			when 'BTC'
				return 0.001
			when 'BCH'
				return 0.002
			when 'LINK'
				return 0.01
			when 'COMP'
				return 0.001
			when 'CRV'
				return 0.2
			when 'DAI'
				return 0.1
			when 'MANA'
				return 10
			when 'ETH'
				return 0.001
			when 'FIL'
				return 0.001
			when 'KEEP'
				return 1
			when 'KNC'
				return 0.1
			when 'LTC'
				return 0.002
			when 'MKR'
				return 0.001
			when 'NXM'
				return 0.01
			when 'OXT'
				return 1
			when 'PAXG'
				return 0.0001
			when 'REN'
				return 1
			when 'STORJ'
				return 1
			when 'SNX'
				return 0.1
			when 'UMA'
				return 0.02
			when 'UNI'
				return 0.1
			when 'YFI'
				return 0.00002
			when 'ZEC'
				return 0.002
			when 'ACH'
				return 0
			when 'GUSD'
				return 0
			when 'USD'
				return 0
			when 'BNT'
				return 0.02
			when '1INCH'
				return 0.1
			when 'LRC'
				return 1
			when 'ENJ'
				return 0.2
			when 'GRT'
				return 0.2
			when 'SAND'
				return 1
			when 'SKL'
				return 1
			else
				return 99999
				# raise "Not implemented"
			end
		end

		def min_vol(pair)
			return SATOSHI
		end

		def min_quantity(pair)
			pair = get_active_pair(pair)
			return pair_detail(pair)[:min_order_size] || raise("Not implement #{pair}")
		end

		def quantity_step(pair)
			pair = get_active_pair(pair)
			return pair_detail(pair)[:quantity_step] || raise("Not implement #{pair}")
		end

		def price_step(pair)
			pair = get_active_pair(pair)
			return pair_detail(pair)[:price_step] || raise("Not implement #{pair}")
		end

		def pair_detail(pair, opt={})
			@gemini_pair_info ||= {}
			return @gemini_pair_info[pair] if @gemini_pair_info[pair] != nil
			res = gemini_req "/v1/symbols/details/#{gemini_symbol(pair)}", public:true, allow_fail:true, silent:opt[:silent]
			raise APD::ExpireResultFailed.new if res.nil? && opt[:allow_fail] == true
			@gemini_pair_info[pair] = {
				:min_order_size => res['min_order_size'].to_f,
				:quantity_step => res['tick_size'].to_f,
				:price_step => res['quote_increment'].to_f
			}
		end
		expire_every MARKET_ASSET_FEE_CACHE_T, :pair_detail

		def _gemini_parse_rules # Suggest using pair_detail()
			# https://docs.gemini.com/rest-api/#symbols-and-minimums
			# Also, could query one by one from https://docs.gemini.com/rest-api/#symbol-details
			fpath = "#{URN::ROOT}/res/gemini_rules.txt"
			raise "No #{fpath}" unless File.file?(fpath)
			gemini_pair_info = {}
			File.read(fpath).split("\n").each_with_index { |line, i|
				next if i == 0
				next if line.size < 10
				segs = line.split("\t").map { |s| s.strip }
				pair = gemini_standard_pair(segs[0])
					gemini_pair_info[pair] = {
						:min_order_size => segs[1].split(' ')[0].strip.to_f, # 0.00001 BTC (1e-5)
						:quantity_step => segs[2].split(' ')[0].strip.to_f, # 0.000001 ZEC (1e-6)
						:price_step => segs[3].split(' ')[0].strip.to_f, # 0.00001 BTC (1e-5)
					}
			}
			# puts JSON.pretty_generate(gemini_pair_info)
			@gemini_pair_info = gemini_pair_info
		end

		def fee_rate_real_evaluate(pair, opt={})
			pair = get_active_pair(pair)
			highest_fee_map = {
				'maker/buy' => 0.10/100,
				'maker/sell' => 0.10/100,
				'taker/buy' => 0.35/100,
				'taker/sell' =>0.35/100 
			}
			first_time = @_fee_rate_real.nil?
			@_fee_rate_real ||= {}
			res = redis_cached_call('notional_volume', 3600) {
				gemini_req '/v1/notionalvolume', allow_fail:true, silent:opt[:silent]
			}
			if res.nil?
				if opt[:allow_fail] == true
					raise APD::ExpireResultFailed.new
				else
					# If allow_fail is false, force using max fee if failed, don't block main process.
					puts "Fallback to highest fee rate: #{pair}"
					@_fee_rate_real[pair] = highest_fee_map
					return @_fee_rate_real[pair]
				end
			end
			puts res if first_time

			# Fee info is contained in JSON
			raise "No api_maker_fee_bps" if res['api_maker_fee_bps'].nil?
			raise "No api_taker_fee_bps" if res['api_taker_fee_bps'].nil?
			maker_fee = res['api_maker_fee_bps'].to_f / 10000.0
			taker_fee = res['api_taker_fee_bps'].to_f / 10000.0
			map = {
				'maker/buy' => maker_fee,
				'maker/sell' => maker_fee,
				'taker/buy' => taker_fee,
				'taker/sell' => taker_fee
			}
			map = map.to_a.map { |kv| [kv[0], kv[1].round(10)] }.to_h
			@_fee_rate_real[pair] = map
		end
		expire_every MARKET_ASSET_FEE_CACHE_T, :fee_rate_real_evaluate

		def support_deviation?(pair)
			true
		end
		
		def initialize(opt={})
			@could_be_banned = true
			@GEMINI_API_DOMAIN = ENV['GEMINI_API_DOMAIN'] || raise('GEMINI_API_DOMAIN is not set in ENV')
			@GEMINI_API_KEY = ENV['GEMINI_API_KEY'] || raise('GEMINI_API_KEY is not set in ENV')
			@GEMINI_API_SEC = ENV['GEMINI_API_SEC'] || raise('GEMINI_API_SEC is not set in ENV')
			@http_proxy_str = @GEMINI_API_PROXY = (ENV['GEMINI_API_PROXY'] || 'default').
				split(',').
				map { |str| str=='default'?nil:str }
			_gemini_parse_rules()
			super(opt)
		end

		def _gemini_account_list(opt={})
			res = gemini_req '/v1/account/list', opt
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res) if @verbose
			res
		end

		def balance(opt={})
			verbose = @verbose && opt[:verbose] != false
			account_list = _gemini_account_list(opt)
			return nil if account_list.nil? && opt[:allow_fail] == true
			raise "Must have a primary account" if account_list.select { |a| a['account'] == 'primary' }.empty?
			# puts JSON.pretty_generate(account_list)

			res = gemini_req '/v1/balances', opt
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res) if @verbose
			cache = res.map { |r|
				asset = r['currency'].upcase
				ttl = r['amount'].to_f
				available = r['available'].to_f
				[asset, {
						'cash'=>available,
						'reserved'=>ttl-available
				}]
			}.to_h
			cache = cache.to_a.select do |kv|
				r = kv[1]
				r['cash'] > 0 || r['reserved'] > 0
			end.to_h
			balance_cache_write cache
			balance_cache_print if verbose
			@balance_cache
		end

		def generate_client_oid(o)
			s = o['client_oid']
			if s.nil?
				# a-zA-Z0-9-_ {1,36}
				s = "#{o['pair']}_#{(Time.now.to_f*1_000_000_000).to_i % 2_147_483_648}_#{rand(9999)}"
				puts "client order id: #{s} generated"
			else
				puts "client order id: #{s} exists"
			end
			s
		end

		def place_order(pair, order, opt={})
			order = order.clone
			pre_place_order(pair, order)
			client_oid = generate_client_oid(order)
			args = {
				:client_order_id	=> client_oid,
				:symbol		=> gemini_symbol(pair),
				:side			=> order['T'], # buy/sell
				:type			=> 'exchange limit',
				:options  => [],
				:amount		=> format_size_str(pair, order['T'], order['s'], adjust:true, verbose:true),
				:price		=> format_price_str(pair, order['T'], order['p'], verbose:true)
			}
			args[:options].push("maker-or-cancel") if opt[:tif] == 'PO'
			args[:options].push("immediate-or-cancel") if opt[:tif] == 'IOC'
			args[:options].push("fill-or-kill") if opt[:tif] == 'FOK'
			args.delete(:options) if args[:options].empty? # An optional array containing at most one supported order execution option. 

			json, place_time_i = nil, nil

			begin # place_time_i might be changed
				place_time_i = (Time.now.to_f - 2) * 1000 # ms
				json = _async_operate_order(pair, client_oid, :new) {
					gemini_req '/v1/order/new', place_order:true, args:args, allow_fail:opt[:allow_fail]
				}
				return nil if json.nil? && opt[:allow_fail] == true
				raise OrderMightBePlaced.new if opt[:test_order_might_be_place] == true
			rescue OrderMightBePlaced => e
				cost_s = (Time.now.to_f - place_time_i/1000 - 2)
				sleep_s = [150 - cost_s, 5].max # 19:28:43 placed -> 19:30:42 appearred
				puts "Order might be placed, cost #{cost_s}s, waiting order in #{sleep_s}s"
				trade = oms_wait_trade(pair, client_oid, sleep_s, :query_new)
				if trade != nil
					post_place_order(trade)
					return trade
				elsif is_banned?() && opt[:allow_fail] == true
					puts "OrderMightBePlaced in banned case, treat it as failed"
					return nil
				end

				query_o = order.clone # Avoid overwriting order
				query_o['client_oid'] = client_oid
				start_t = Time.now.to_f
				loop {
					target = nil
					begin
						target = query_order(pair, query_o, allow_fail: true, just_placed:true)
					rescue OrderNotExist
						puts "Order not exsit".red
						break
					end
					if target.nil? # Failed
						query_ttl_t = Time.now.to_f - start_t
						if query_ttl_t > 600 && opt[:allow_fail] == true
							puts "Too much time in querying this client_oid:\n#{JSON.pretty_generate(query_o)}"
							break
						else
							sleep 60
							next
						end
					end
					post_place_order(target)
					return target
				}

				return nil if opt[:allow_fail] == true
				puts "No his exists, re-place order after.".blue
				retry
			rescue OrderArgumentError => e # Maybe price is too high, or size is zero.
				return nil if opt[:allow_fail] == true
				raise e
			end
			record_operation_time
			trade = gemini_normalize_trade pair, json
			if client_oid != trade['client_oid'] # Defensive check
				raise "client_oid not consistent #{client_oid} #{trade['client_oid']}\n#{trade.to_json}"
			end
			post_place_order(trade)
			# Gemini wont complain this error directly.
			raise NotEnoughBalance.new(JSON.pretty_generate(trade)) if trade['type'] == 'rejected' && trade['reason'] == 'InsufficientFunds'
			trade
		end

		def query_order(pair, order, opt={})
			verbose = @verbose && opt[:verbose] != false
			args = { :include_trades => false }
			if order['client_oid'] != nil
				args['client_order_id'] = order['client_oid']
			elsif order['i'] != nil
				args['order_id'] = order['i']
			end

			json = nil
			wait_gap = 1
			begin
				if pair.nil? # Pair could be null, will not able to use OMS in this case.
					puts ">> #{order['i']} #{order['client_oid']}, skip OMS bc no pair"
					oms_missed = true
					res = gemini_req '/v1/order/status', args: args, allow_fail:opt[:allow_fail], silent:opt[:silent]
					res = res[0] if res.is_a?(Array) && res.size == 1
					json = res
				else
					order['pair']	= pair
					json = oms_order_info(
						pair,
						(order['i'] || order['client_oid'] || raise("No id in #{order.to_json}")),
						opt
					)
					oms_missed = false
					if json.nil?
						oms_missed = true
						puts ">> #{order['i']} #{order['client_oid']}" if verbose

						if opt[:just_placed]
							json = _async_operate_order(pair, order['client_oid'] || order['i'], :query_new) {
								res = gemini_req '/v1/order/status', args: args, allow_fail:opt[:allow_fail], silent:opt[:silent]
								res = res[0] if res.is_a?(Array) && res.size == 1
								res
							}
						else
							res = gemini_req '/v1/order/status', args: args, allow_fail:opt[:allow_fail], silent:opt[:silent]
							res = res[0] if res.is_a?(Array) && res.size == 1
							json = res
						end
					end
				end
			rescue OrderNotExist => e
				if opt[:just_placed] # Sometimes order args have ['t'] too, cloned from other orders.
					;
				elsif order['t'] != nil && order_age(order) >= 1*3600_000
					puts "Order is too old, mark this as closed\n#{format_trade(order)}"
					order_set_dead(order)
					post_cancel_order(order, order)
					return order
				end
				raise e if order['i'].nil? # Pending orders
				return nil if opt[:allow_fail] == true
				keep_sleep wait_gap
				wait_gap = [wait_gap+1, 20].min
				raise e if wait_gap == 20 # Terminate barrier
				retry
			end
			return nil if json.nil? && opt[:allow_fail] == true
			trade = gemini_normalize_trade pair, json
			oms_order_write_if_null(pair || order['pair'], order['i'], trade) if oms_missed
			post_query_order(order, trade, opt)
			trade
		end

		def cancel_order(pair, order, opt={})
			pre_cancel_order(order)
			args = { :order_id => order['i'] }
			begin
				json = _async_operate_order(pair, order['i'], :cancel) {
					gemini_req '/v1/order/cancel', args: args, allow_fail:opt[:allow_fail], cancel_order:true
				}
				return nil if json.nil? && opt[:allow_fail] == true
			rescue OrderNotExist => e
				if order['t'] != nil && order_age(order) <= 20_000 # Longest latency seen: 10s
					puts "order is pretty new < 20 seconds, treat this as an error"
					return nil if opt[:allow_fail] == true
					puts "Retry after 3 seconds"
					keep_sleep 3
					retry
				end
				raise e
			end
			record_operation_time

			# In case of buffer latency. Wait OMS for longer time.
			trade = oms_wait_trade(pair, order['i'], 5, :cancel, force_update:order)
			return nil if trade.nil? && opt[:allow_fail] == true

			if order_alive?(trade)
				puts "Order is still alive"
				oms_order_delete(pair, order['i'])
				return nil if opt[:allow_fail] == true
				puts "cancel it again"
				return cancel_order(pair, order, opt)
			end

			post_cancel_order(order, trade)
			trade
		end

		def active_orders(pair, opt={})
			verbose = @verbose && opt[:verbose] != false
			json = gemini_req '/v1/orders', opt
			return nil if json.nil? && opt[:allow_fail] == true
			orders = []
			json.each { |o|
				o = gemini_normalize_trade(nil, o)
				if pair.nil?
					print "#{o['pair']}\n" if verbose
				else
					next if pair != o['pair']
				end
				print "#{format_trade(o)}\n" if verbose
				orders.push o
			}
			orders
		end

		# Gemini only provides history trades, we need to rebuild history orders.
		def recent_orders(pair, opt={})
			verbose = @verbose && opt[:verbose] != false

			# Get all open orders.
			open_orders = active_orders(pair, opt)
			return nil if open_orders.nil? && opt[:allow_fail] == true
			order_by_id = {}
			open_orders.each { |o| order_by_id[o['i']] = o }

			# Get all trades, extract order ids, then query them one by one
			args = { :limit_trades => 100 } # Max 500
			args[:symbol] = gemini_symbol(pair) if pair != nil
			if opt[:timestamp] != nil # in second. Only return trades on or after this timestamp
				args[:timestamp] = opt[:timestamp]
			end
			json = gemini_req '/v1/mytrades', args: args, allow_fail:opt[:allow_fail]
			return nil if json.nil? && opt[:allow_fail] == true
			puts "#{json.size} recent trades got" if @verbose
			trades = json.select { |t|
				# puts t.to_json
				# "price":"0.05972","amount":"0.132","timestamp":1627261408,"timestampms":1627261408334,
				# "type":"Sell","aggressor":false,"fee_currency":"BTC","fee_amount":"0.00000788304",
				# "tid":49423497780,"order_id":"49423497317","exchange":"gemini","is_auction_fill":false,
				# "is_clearing_fill":false,"symbol":"ETHBTC","client_order_id":"BTC-ETH_1678819584_967"
				next true if pair.nil?
				next gemini_standard_pair(t['symbol']) == pair
			}
			trades.each_with_index { |t, i|
				next if order_by_id[t['order_id']] != nil # Skip known orders.
				cached_json = URN::OMSLocalCache.oms_info(market_name(), t['order_id'])
				if cached_json != nil
					cached_o = _normalize_trade(nil, JSON.parse(cached_json))
					order_by_id[t['order_id']] = cached_o
				else
					new_order = { 'pair' => pair, 'i' => t['order_id'], 'market' => 'Gemini' }
					puts "Querying #{i}/#{trades.size} OMS support? #{URN::OMSLocalCache.support_mkt?(market_name())}"
					new_order = query_order(pair, new_order, opt)
					return nil if new_order.nil? && opt[:allow_fail] == true
					order_by_id[t['order_id']] = new_order
					print "#{new_order['pair']}\n" if verbose && pair.nil?
					print "#{format_trade(new_order)}\n" if verbose
				end
			}

			orders = order_by_id.values.sort_by { |o| o['t'] }
			puts "Recent orders:" if verbose
			orders.each do |o|
				print "#{o['pair']}\n" if verbose && pair.nil?
				print "#{format_trade(o)}\n" if verbose
			end
			orders
		end

		def history_orders(pair, opt={})
			orders = recent_orders(pair, opt)
			return nil if orders.nil? && opt[:allow_fail] == true
			orders.select { |o| order_alive?(o) == false }
		end

		def all_pairs(opt={})
			# Also: https://docs.gemini.com/rest-api/#symbols
			@gemini_pair_info.keys.map { |pair|
				[pair, gemini_symbol(pair)]
			}.to_h
		end
		expire_every MARKET_PAIRS_CACHE_T, :all_pairs

		#############################################
		# Withdraw and deposit
		#############################################
		GEMINI_NETWORK_MAP = {
			'BTC' => 'bitcoin',
			'ETH' => 'ethereum',
			'BCH' => 'bitcoincash',
			'LTC' => 'litecoin',
			'ZEC' => 'zcash',
			'FIL' => 'filecoin'
		}
		def deposit_addr(asset, opt={})
			asset = asset.upcase
			network = GEMINI_NETWORK_MAP[asset]
			network = GEMINI_NETWORK_MAP['ETH'] if URN::ETH_TOKENS.include?(asset)
			raise "No known network for #{asset}" if network.nil?
			res = gemini_req "/v1/addresses/#{network}", allow_fail:opt[:allow_fail]
			return nil if res.nil? && opt[:allow_fail] == true
			raise "No addr" if res.nil?
			raise "Multiple deposit addr for #{asset}" if res.size > 1
			raise "Zero deposit addr for #{asset}" if res.empty?
			addr = res[0]['address']
			puts "#{asset} addr: #{addr}" if @verbose
			valid_addr?(addr)
			addr
		end

		def withdraw(asset, amount, address, opt={})
			# Need a whitelist
			raise "Cryptocurrency withdrawal address whitelists are not enabled for account primary. Please contact support@gemini.com for information on setting up a withdrawal address whitelist."
			amount = amount.round(6)
			asset = asset.upcase
			asset = 'USD' if asset == 'GUSD' # https://docs.gemini.com/rest-api/#withdraw-usd-as-gusd
			args = {
				:address	=> address,
				:amount => amount.to_s
			}
			raise "Not implemented: withdraw with memo" if opt[:message] != nil
			puts args
			puts "#{asset} WITHDRAW #{amount} -> #{address} #{opt[:message]}".red
			valid_addr?(address)
			puts "Fire in 10 seconds:\n#{JSON.pretty_generate(args)}".red
			keep_sleep 10
			res = nil
			begin
				res = gemini_req "/v1/withdraw/#{asset.downcase}", args: args, allow_fail:opt[:allow_fail]
			rescue => e
				raise e
			end
			puts JSON.pretty_generate(res)
			return nil if res.nil? && opt[:allow_fail] == true
			raise "response nil" if res.nil?
			raise res.to_json if res['withdrawalId'].nil?
			res['withdrawalId']
		end

		def transactions(asset, opt={})
			asset = asset.upcase
			limit = opt[:limit] || 3
			his = nil
			loop do
				# Max records 50
				his = gemini_req('/v1/transfers', limit_transfers: 50, allow_fail:opt[:allow_fail], silent:opt[:silent]) || []
				his = his.map { |t|
					t['xfr_id'] = t.delete('eid')
					t['finished'] = (t['status'] == 'Complete' || t['status'] == 'Advanced')
					t['asset'] = t.delete('currency')
					t['t'] = t.delete('timestampms').to_i
					t['address'] = t.delete('destination')
					t['amount'] = t.delete('amount').to_f
					t['txid'] = t.delete('txHash')

					if t['type'] == 'Deposit'
						t['type'] = 'deposit'
					elsif t['type'] == 'Withdrawal'
						t['type'] == 'withdraw'
					else
						raise "Unknown transaction type in #{t.to_json}"
					end

					if t['asset'] == 'USD' && t['txid'] != nil && t['address'] != nil
						t['asset'] = 'GUSD' # On-chain TX for USD is GUSD token
					end

					t
				}.select { |t| t['asset'] == asset }.sort_by { |t| t['t'] }.reverse
				his = his[0..(limit-1)]
				break unless opt[:watch] == true
				puts JSON.pretty_generate(his)
				keep_sleep 10
			end
			his
		end

		def market_summary(pair, opt={})
			res = redis_cached_call('candles_1day_'+pair, 60) {
				gemini_req "/v2/candles/#{gemini_symbol(pair)}/1day", public:true, allow_fail: opt[:allow_fail]
			}
			return if res.nil? && opt[:allow_fail] == true
			d = res.sort_by { |r| r[0] }.last
			time, open, high, low, close, vol = d[0..5]
			chg = close/open - 1
			{
				'from'					=> DateTime.now - 1,
				'open'					=> open,
				'last'					=> close,
				'high'					=> high,
				'low'						=> low,
				'amt'						=> vol,
				'vol'						=> vol*close
			}
		end

		def market_summary_v1(pair, opt={})
			res = redis_cached_call('pricefeed', 60) {
				gemini_req "/v1/pricefeed", public:true, allow_fail: opt[:allow_fail]
			}
			return if res.nil? && opt[:allow_fail] == true
			d = res.select { |x| x['pair'] == gemini_symbol(pair) }.first
			return nil if d.nil?
			chg = d['percentChange24h'].to_f / 100
			last = d['price'].to_f
			{
				'from'					=> DateTime.now - 1,
				'open'					=> last/(1+chg), 
				'last'					=> last,
				'high'					=> last,
				'low'						=> last
			}
		end

		def market_summaries
			res = redis_cached_call('pricefeed', 60) {
				gemini_req "/v1/pricefeed", public:true, allow_fail: opt[:allow_fail]
			}
			return if res.nil? && opt[:allow_fail] == true
			res.map { |d|
				pair = gemini_symbol(d['pair'])
				chg = d['percentChange24h'].to_f / 100
				last = d['price'].to_f
				[pair, {
					'last' => last,
					'open' => last/(1+chg), 
					'chg' => chg,
					'vol' => 0
				}]
			}.to_h
		end

		def pair_to_underlying_pair(pair) # Dirty tricks to mapping pair -> trading pair
			if pair =~ /^USDT-/
				pair = pair.gsub(/^USDT/, 'USD')
			end
			pair
		end
		def underlying_pair_to_pair(pair) # Dirty tricks to mapping trading pair -> pair
			if ['USD-STORJ', 'USD-CVC'].include?(pair)
				pair = pair.gsub('USD', 'USDT')
			end
			pair
		end

		def earn_balance(opt={})
			verbose = @verbose && opt[:verbose] != false
			res = gemini_req '/v1/balances/earn', opt
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res) if @verbose
			res
		end

		def earn_products(opt={})
			verbose = @verbose && opt[:verbose] != false
			res = gemini_req '/v1/earn/rates/', opt
			return nil if res.nil? && opt[:allow_fail] == true
			puts JSON.pretty_generate(res) if @verbose
			res
		end

		private
		######### Format control #########
		def gemini_symbol(pair)
			pair = pair_to_underlying_pair(pair)
			segs = pair.split('-')
			(segs[1] + segs[0]).upcase
		end
		def gemini_standard_pair(symbol) # ltcusd -> USD-LTC
			pair = nil
			symbol = symbol.gsub('/', '').gsub('_', '').upcase
			['BTC', 'ETH', 'EUR', 'GBP', 'SGD', 'USD', 'LTC', 'BCH', 'DAI'].each { |base|
				if symbol.end_with?(base)
					end_pos = 0 - base.size - 1
					pair = (base.upcase + '-' + symbol[0..end_pos]).upcase
					break
				end
			}
			raise "Unexpected symbol #{symbol}" if pair.nil?
			pair = underlying_pair_to_pair(pair)
			pair
		end
		# Make order be standard format.
		def gemini_normalize_trade(pair, order)
			return order if order['_parsed_by_uranus'] == true
			raise "Order illegal #{order}" unless order.is_a?(Hash)
			# "order_id": "35038325832",
			# "id": "35038325832"
			order ['i'] = order.delete('order_id').to_s
			order ['client_oid'] = order.delete('client_order_id')
			# "symbol": "btcusd"
			parsed_pair = gemini_standard_pair(order['symbol'])
			raise "Unexpected pair #{pair} - #{JSON.pretty_generate(order)}" if pair != nil && pair != parsed_pair
			order['pair'] = parsed_pair

			# "side": "buy"
			case order['side']
			when 'buy'
				order['T'] = 'buy'
			when 'sell'
				order['T'] = 'sell'
			else
				raise "Unknown order type: #{order.to_json}"
			end

			# "type": "exchange limit" # From API
			# "type": "booked/closed/cancelled..." # From WSS OMS
			if order['type'] == 'accepted' # Status complement
				order['remaining_amount'] ||= order['original_amount']
				order['executed_amount'] ||= 0
			end
			# "order_type": "exchange limit" # from WSS OMS
			# "timestampms": 1618905095648
			order['t'] = order['timestampms'].to_i
			# "price": "10000.00"
			order['p'] = order['price'].to_f
			# "avg_execution_price": "0.00"
			order['avg_price'] = order['avg_execution_price'].to_f
			order['avg_price'] = order['p'] if order['avg_price'] == 0
			# "original_amount": "0.01"
			order['s'] = order['original_amount'].to_f
			# "remaining_amount": "0.01"
			order['maker_size'] = order['remained'] = order['remaining_amount'].to_f
			# "executed_amount": "0"
			order['executed'] = order['executed_amount'].to_f
			if order['remained'] == 0 && order['executed'] == 0
				if order['type'] == 'rejected'
					puts "order rejected #{JSON.pretty_generate(order)}".red
					order['remained'] = order['s']
				else
					raise "Unknown case of order #{JSON.pretty_generate(order)}"
				end
			end

			# Could parse trades list for precise maker_size calculation
			# But trades list is not available from OMS ? check fill event TODO

			# "is_live": true
			# "is_cancelled": false
			case [order['is_live'], order['is_cancelled']]
			when [true, false]
				order['status'] = 'new'
			when [false, true]
				order['status'] = 'canceled'
			when [false, false]
				if order['type'] == 'rejected'
					order['status'] = 'canceled'
				elsif order['type'] == 'closed' # Closed after maintain.
					order['status'] = 'canceled'
				elsif order['remained'] == 0
					order['status'] = 'finished'
				else
					raise "Unexpected status #{JSON.pretty_generate(order)}"
				end
			else
				raise "Unexpected status #{JSON.pretty_generate(order)}"
			end

			order['market']	= 'Gemini'
			order_status_evaluate(order)
			order
		end
		alias_method :_normalize_trade, :gemini_normalize_trade

		######### Rate limit control #########
		# https://docs.gemini.com/rest-api/#rate-limits
		# 	For public API entry points, we limit requests to 120 requests per minute, and recommend that you do not exceed 1 request per second.
		# 	For private API entry points, we limit requests to 600 requests per minute, and recommend that you not exceed 5 requests per second.
		#
		# 	Example: 600 requests per minute is ten requests per second, meaning one request every 0.1 second.
		#
		# 	If you send 20 requests in close succession over two seconds, then you could expect:
		#
		# 	the first ten requests are processed
		# 	the next five requests are queued
		# 	the next five requests receive a 429 response, meaning the rate limit for this group of endpoints has been exceeded
		# 	any further incoming request immediately receive a 429 response
		# 	after a short period of inactivity, the five queued requests are processed
		# 	following that, incoming requests begin to be processed at the normal rate again
		def api_rate_rule
			return {
				'rule' => {
					'weight' => [12, 1.2], # burst 10~15
					'order' => [12, 1.2]
				},
				'score' => {
					'weight' => 12,
					'order' => 12
				},
				'his' => [],
				'extra' => []
			}
		end

		private
		def wss_key
			now = Time.now
			priv_payload =  {
				"request" => '/v1/order/events',
				"account" => "primary",
				"nonce" => (now.to_f * 1_000_000).to_i.to_s
			}
			payload_json_b64 = Base64.strict_encode64(priv_payload.to_json)
			signature = OpenSSL::HMAC.hexdigest(@sha384_digest, @GEMINI_API_SEC, payload_json_b64)

			{
				'pair_map' => all_pairs(),
				'header' => {
					:'Content-Type' => "text/plain",
					:'Content-Length' => "0",
					:'X-GEMINI-APIKEY' => @GEMINI_API_KEY,
					:'X-GEMINI-PAYLOAD' => payload_json_b64,
					:'X-GEMINI-SIGNATURE' => signature,
					:'Cache-Control' => "no-cache"
				}
			}.to_json
		end

		def gemini_req(path, opt={})
			args = opt[:args] || {}
			timeout = 10
			timeout = 10 if opt[:place_order] == true
			req_time = nil
			original_path = path
			loop do
				path = original_path
				if opt[:place_order] == true || opt[:cancel_order] == true
					return nil if opt[:allow_fail] == true && is_banned?()
					wait_if_banned()
				end

				now = Time.now
				payload = {}
				header = {}
				if opt[:public] == true
					# Public APIs are accessible via GET, and the parameters for the request are included in the query string.
					method = :GET
					display_args_str = args.to_a.
						sort_by { |kv| kv[0] }.
						map { |kv| kv[0].to_s + '=' + kv[1].to_s }.join('&')
					path = "#{path}?#{display_args_str}"
				else
					# Authenticated APIs do not submit their payload as POSTed data, but instead put it in the X-GEMINI-PAYLOAD header
					priv_payload =  {
						"request" => path,
						"account" => "primary",
						"nonce" => (now.to_f * 1_000_000).to_i.to_s
					}
					args.each { |k, v| priv_payload[k] = v }
					payload_json_b64 = Base64.strict_encode64(priv_payload.to_json)
					signature = OpenSSL::HMAC.hexdigest(@sha384_digest, @GEMINI_API_SEC, payload_json_b64)

					header = {
						:'Content-Type' => "text/plain",
						:'Content-Length' => "0",
						:'X-GEMINI-APIKEY' => @GEMINI_API_KEY,
						:'X-GEMINI-PAYLOAD' => payload_json_b64,
						:'X-GEMINI-SIGNATURE' => signature,
						:'Cache-Control' => "no-cache"
					}
					method = :POST
				end

				emergency_call = (opt[:cancel_order] == true || opt[:emergency_call] == true)
				memo = "#{path} #{opt[:memo] || display_args_str} #{emergency_call ? "EMG".red : ""}"
				loop {
					break if opt[:skip_api_rate_control] == true
					should_call = api_rate_control(1, emergency_call, memo, opt)
					break if should_call
					if should_call == false && opt[:allow_fail] == true
						puts "Abort request because of rate control"
						return nil
					end
					puts "Should not call api right now, wait: #{path} #{display_args_str}"
					keep_sleep 1
				}

				url = "#{@GEMINI_API_DOMAIN}#{path}"
				begin
					payload = nil if payload == {}
					response, proxy = mkt_http_req(
						method, url,
						header: header, timeout: timeout,
						payload: payload, display_args: "#{path} #{payload}",
						silent: opt[:silent]
					)
					begin
						return JSON.parse(response)
					rescue # JSON format error, when network is broken, response could be ''
						raise OrderMightBePlaced.new if opt[:place_order] == true
						keep_sleep 1
						next if response.size < 2
						raise "Unable to parse #{response}"
					end
				rescue Zlib::BufError, SOCKSError, RestClient::Exception, Net::HTTPBadResponse, Net::HTTPFatalError, HTTP::Error => e
					puts "--> #{url}".red
					err_msg, err_res = '', ''
					if e.is_a?(RestClient::Exception)
						puts ['API failed', opt, e.message, e.response.to_s]
						err_msg, err_res = e.message.to_s, e.response.to_s
					else
						puts ['API failed', e.class, e.message]
						err_msg = e.message.to_s
					end

					err_json = nil
					err_json = no_complain { JSON.parse(err_res) }

					case (err_json || {})['reason']
					when 'InvalidNonce'
						return nil if opt[:allow_fail] == true
						keep_sleep 3
						next
					when 'Maintenance'
						now = DateTime.now
						puts "System abnormality, maybe exchange is under maintenance."
						t = banned_util()
						if t.nil? || t < (now + 30.0/86400.0)
							# Wait 30-90 seconds
							t = (now + (30.0+Random.rand(90))/86400.0)
							set_banned_util(t, "#{err_msg} #{err_res}")
						end
					when 'InsufficientFunds'
						raise NotEnoughBalance.new(err_res)
					when 'InvalidPrice'
						raise OrderArgumentError.new(err_res)
					when 'InvalidQuantity'
						raise OrderArgumentError.new(err_res)
					when 'InvalidSymbol'
						raise ActionDisabled.new(err_res)
					when /(Maintenance|System|MarketNotOpen)/
						broadcast = ((err_json || {})['reason'].include?('MarketNotOpen') == false)
						t = banned_util()
						now = DateTime.now
						if t.nil? || t < (now + 30.0/86400.0)
							# Wait 30-90 seconds
							t = (now + (30.0+Random.rand(90))/86400.0)
							# Maybe only this market is offline, dont broadcast.
							set_banned_util(t, "#{err_msg} #{err_res}", broadcast:broadcast)
						end
						return nil if opt[:allow_fail] == true
					when 'OrderNotFound'
						raise OrderNotExist.new(err_res)
					when 'RateLimit'
						t = banned_util()
						now = DateTime.now
						if t.nil? || t < (now + 3.0/1440.0)
							# Wait 1-2 min
							t = (now + (60.0+Random.rand(60))/14400.0)
							set_banned_util(t, "#{err_msg} #{err_res}")
						end
						return nil if opt[:allow_fail] == true
						next
					when 'System'
						# We are investigating technical issues with the Gemini Exchange.
						# Please check https://status.gemini.com/ for more information.
						t = banned_util()
						now = DateTime.now
						if t.nil? || t < (now + 3.0/1440.0)
							# Wait 1-2 min
							t = (now + (60.0+Random.rand(60))/14400.0)
							set_banned_util(t, "#{err_msg} #{err_res}")
						end
						return nil if opt[:allow_fail] == true
						keep_sleep 60
						next
					else
						if err_res.empty? || err_json.nil?
							; # Empty response caused by timeout or other reasons.
						else
							puts "Unexpected error code [#{err_json}]"
							raise e
						end
					end

					raise OrderMightBePlaced.new if opt[:place_order] == true
					return nil if opt[:allow_fail] == true
					keep_sleep 3
					next if e.is_a?(HTTP::Error)
					next if normal_api_error?(e)
					raise e
				rescue ThreadError => e
					puts e.message
					keep_sleep 1
					next if (e.message || '').include?('Resource temporarily unavailable')
					raise e
				rescue OpenSSL::SSL::SSLError, Errno::ECONNREFUSED, SocketError, Errno::EHOSTUNREACH, Errno::ETIMEDOUT, Errno::ENETUNREACH, Errno::ECONNRESET, Errno::EPIPE => e
					puts e.message
					return nil if opt[:allow_fail] == true
					next
				end
			end
		end
	end
end

######### TEST #########
if __FILE__ == $0 && defined? URN::BOOTSTRAP_LOAD
	client = URN::Gemini.new verbose:true, skip_balance:true
	client.run_cli
end

