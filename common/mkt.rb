require_relative '../common/bootstrap'
module URN
	# Normally for market client to keep orders in manintain,
	# Keep a balance cache for single pair.
	module BalanceManager
		include URN::OrderUtil
		include APD::LockUtil

		def balance_cache_write(cache)
			@balance_cache_init_time = DateTime.now if @balance_cache.nil?
			@balance_cache = cache
		end

		# Put order into {pair=>{id=>order}} cache,
		# replace and return its old copy if exists.
		def order_remember(o, opt={})
			raise "order should have i #{o}" if o['i'].nil?
			puts "order_remember:\n#{format_trade(o)}" if opt[:verbose] == true
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= {}
			old_order = @managed_orders.dig(o['pair'], o['i'])
			if old_order != nil
				unless order_same?(old_order, o) # Defensive check
					raise "old_order and o aren't same\n#{old_order}\n#{o}"
				end
				puts "order_replace:\n#{format_trade(ord)}" if opt[:verbose] == true
			end
			@managed_orders[o['pair']][o['i']] = o
			old_order
		end

		# Delete order from self managed {pair=>[orders]} cache.
		def order_forget(o)
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= {}
			@managed_orders[o['pair']].delete(o['i'])
		end

		# Purge self managed order cache.
		def order_forget_all
			@managed_orders = {}
		end

		def order_managed?(o)
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= {}
			@managed_orders[o['pair']][o['i']] != nil
		end
		thread_safe :order_remember, :order_forget, :order_forget_all, :order_managed?

		# Put order into {pair=>[orders]} cache, replace its old copy if exists.
		# Return replaced (old version) order.
		def _old_order_remember(o, opt={})
			puts "order_remember:\n#{format_trade(o)}" if opt[:verbose] == true
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= []
			old_order = nil
			@managed_orders[o['pair']] = @managed_orders[o['pair']].delete_if do |ord|
				if order_same?(ord, o)
					puts "order_replace:\n#{format_trade(ord)}" if opt[:verbose] == true
					if old_order.nil?
						old_order = ord
						next true
					end
					raise "#{market_name()} Duplicated order in @managed_orders?\n#{@managed_orders.to_json}"
				end
				next false
			end
			@managed_orders[o['pair']].push o
			old_order
		end

		# Delete order from self managed {pair=>[orders]} cache.
		def _old_order_forget(o)
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= []
			@managed_orders[o['pair']].delete_if { |ord| order_same?(ord, o) }
		end

		def _old_order_managed?(o)
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= []
			@managed_orders[o['pair']].select { |ord| order_same?(ord, o) }.size > 0
		end

		# Support spot and BTC-based future.
		#
		# If price is given, all available cash will transform into quantity.
		# For spot market:
		# 	For buying order: return available cash (base-currency) / price
		# 	For selling order: max quantity of asset that could be sell
		#
		# For BTC based future market (Bitmex):
		# Future markets should check position first.
		# New order should close current position first,
		# Position cost of each pair should not larger than future_max_position_cost()
		# 	When holding NO position:
		# 		For buying order: return available cash (base-currency) / price
		# 		For selling order: return available cash (base-currency) / price
		# 	When holding long position:
		# 		For buying order: return available cash (base-currency) / price
		# 		For selling order: quantity of long position that could be closed
		# 	When holding short position:
		# 		For buying order: quantity of short position that could be closed
		# 		For selling order: return available cash (base-currency) / price
		#
		# Don't use this directly, it is designed for max_order_size()
		# To get balance:
		# Try reading @balance_cache directly.
		# For placing orders:
		# Try max_order_size(order) instead.
		def _balance_avail(pair, type, price, opt={})
			balance() if @balance_cache.nil?
			raise "Price should be specfied" if price.nil?
			asset1, asset2 = pair_assets(pair)

			if market_type() == :spot
				# For spot market, keep balance 0.01 by default
				remain = opt[:clear_bal] == true ? 0 : 0.01
				if ['buy', 'bid'].include?(type.strip.downcase)
					cash = (@balance_cache.dig(asset1, 'cash') || 0) - remain
					return cash/price
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					return (@balance_cache.dig(asset2, 'cash') || 0) - remain
				end
				raise "unknown type #{type}"
			elsif market_type() == :future
				; # Process this below.
			else
				raise "unknown market type #{market_type()}"
			end

			########### Process for future markets ############
			cash_in_asset1 = true
			if market_name() == 'Bitmex'
				cash_in_asset1 = true
				raise "Base asset should be BTC #{pair}" unless asset1 == 'BTC'
			elsif market_name() == 'HBDM' || market_name() == 'Bybit'
				cash_in_asset1 = false
				raise "Base asset should be USD #{pair}" unless asset1 == 'USD'
			else
				raise "Unknown future market #{market_name()}"
			end
			# Don't reduce cash by remain for futures.
			# Position cost of each pair should not larger than future_max_position_cost()
			cash = future_available_cash(pair, type)
			position = future_position(pair)
			if position == 0
				if ['buy', 'bid'].include?(type.strip.downcase)
					lev = future_max_long_leverage(pair)
					if cash_in_asset1 # Use A to long A-B
						cash /= lev
						return cash/price
					end
					return cash*(lev-1) # Use B to long A-B
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					lev = future_max_short_leverage(pair)
					if cash_in_asset1 # Use A to short A-B
						cash /= lev
						return cash/price
					end
					return cash*(lev+1) # Use B to short A-B
				else
					raise "unknown type #{type}"
				end
			elsif position > 0 # Long position
				if ['buy', 'bid'].include?(type.strip.downcase)
					lev = future_max_long_leverage(pair)
					if cash_in_asset1
						cash /= lev
						return cash/price
					end
					return cash*(lev-1)
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					return position
				end
				raise "unknown type #{type}"
			elsif position < 0 # Short position
				if ['buy', 'bid'].include?(type.strip.downcase)
					return 0-position
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					lev = future_max_short_leverage(pair)
					if cash_in_asset1
						cash /= lev
						return cash/price
					end
					return cash*(lev+1)
				end
				raise "unknown type #{type}"
			end
		end

		# Max long position could be open with 1 asset
		def future_max_long_leverage(pair=nil)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			return 1.4 if market_name() == 'HBDM' # Remove this when altcoins prices are high
			1.2
		end

		# Max short position could be open with 1 asset
		def future_max_short_leverage(pair=nil)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			2
		end

		def future_position(pair)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			asset1, asset2 = pair_assets(pair)
			(@balance_cache.dig(asset2, 'cash') || 0) + (@balance_cache.dig(asset2, 'reserved') || 0)
		end

		def future_side_for_close(pair, side)
			return false unless market_type() == :future
			position = future_position(pair)
			return true if position > 0 && side == 'sell'
			return true if position < 0 && side == 'buy'
			return false
		end

		# For future markets(HBDM/Bybit) based on volume, use this for contract position.
		def future_position_cost(pair)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			asset1, asset2 = pair_assets(pair)
			if quantity_in_orderbook() == :asset
				return (@balance_cache.dig(asset2, 'cost') || 0)
			elsif quantity_in_orderbook() == :vol
				return (@balance_cache.dig(asset2, 'cash') || 0) + (@balance_cache.dig(asset2, 'reserved') || 0)
			else
				raise "Unknown quantity_in_orderbook()"
			end
		end

		# Max position cost in BTC for given pair.
		# TODO Better integrate into current preprocess_deviation()
		def future_max_position_cost(pair, type)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			0.01
		end

		# Compute available cash asset for open new position
		# For Bitmex (which uses BTC for *every* contract margin):
		# For HBDM/Bybit (which uses asset2(asset1-asset2) for *related* contract margin):
		# 	Available cash = total_asset - sum(pending_buying_orders)* MAX_LEVERAGE - sum(position_cost x MAX_LEVERAGE)
		# 	Position cost of each pair should not be larger than future_max_position_cost()
		def future_available_cash(pair, type, opt={})
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			cash_asset = nil
			# Bitmex: all contracts use BTC as cash, BTC-TRX position cost in BTC, USD-BTC? TODO
			# Bybit/HBDM: ETH as cash, USD-ETH position cost in USD
			cost_on_base = nil # false -> cash cost count in order size
			if market_name() == 'Bitmex'
				cash_asset = 'BTC'
				raise "Not implemented for #{pair} in Bitmex" unless pair.start_with?(cash_asset)
				cost_on_base = true
			elsif market_name() == 'HBDM' || market_name() == 'Bybit'
				asset1, asset2 = pair_assets(pair.split('@').first)
				cash_asset = asset2 # USD-ETH@expiry -> ETH
				raise "Not implemented for #{pair} in #{market_name()}" unless pair.start_with?("USD-#{cash_asset}")
				cost_on_base = false
			else
				raise "Not implemented for #{market_name()}"
			end
			# Available balance should deduct from pending buying orders.
			cash_balance = @balance_cache.dig(cash_asset, 'cash')
			cash_balance -= (@balance_cache.dig(cash_asset, 'reserved') || 0)*future_max_long_leverage()
			# For future, check it current position first.
			raise "No #{cash_asset} balance from #{@balance_cache}" if cash_balance.nil?
			# Total cost of pair, total cost of long and short
			pair_cost, long_cost, short_cost = 0.0, 0.0, 0.0
			@balance_cache.each do |asset, bal_map|
				next unless bal_map['type'] == 'future' # asset is contract name.
				count_pair_cost = (pair == asset)
				if market_name() == 'HBDM' || market_name == 'Bybit'
					# HBDM/Bybit uses ETH for USD-ETH contracts only
					# USD-ETH@20190927 matches (-ETH@)
					# USD-ETH@20190927 and USD-ETH@20190913 share same margin.
					next unless asset.include?("-#{cash_asset}@")
					count_pair_cost = true
				end
				cash, reserved, cost = ['cash', 'reserved', 'cost'].map do |k|
					raise "No #{asset} #{k} in #{@balance_cache}" if bal_map[k].nil?
					bal_map[k]
				end
				# Asset reserve always means pending selling orders.
				# Total long position = cash + reserve
				# Total short position = cash + reserve
				position = cash + reserved
				# Defensive check # Would cause crash, cost is not maintained along with balance
				# if (position == 0 && cost != 0) || (position != 0 && cost == 0)
				# 	raise "Position and cost unconinstent in #{asset} of #{@balance_cache}"
				# end
				value = 0
				if position > 0 # Long position
					lev = future_max_long_leverage(asset)
					value = cost_on_base ? (cost.abs() * lev) : (position.abs() / (lev-1))
					long_cost += value
					pair_cost += value if count_pair_cost && type == 'buy'
				elsif position < 0 # Short position
					lev = future_max_short_leverage(asset)
					value = cost_on_base ? (cost.abs() * lev) : (position.abs() / (lev+1))
					short_cost += value
					pair_cost += value if count_pair_cost && type == 'sell'
				end
				if opt[:verbose] == true
					puts "Pair #{pair} asset #{asset} cost #{cost} position #{position} -> cost_lev #{value}"
				end
			end
			available_cash = (cash_balance - long_cost - short_cost).floor(8)
			max_cost = future_max_position_cost(pair, type)
			allocate_cash = [available_cash, max_cost-pair_cost].min
			if opt[:verbose] == true
				puts "ALL cost L #{long_cost} S #{short_cost} Pair #{type}: #{pair_cost}"
				puts "Max leverage: L #{future_max_long_leverage()} S #{future_max_short_leverage()}"
				puts "#{cash_asset} #{cash_balance} Avail #{available_cash} Max #{max_cost} Allocate:#{allocate_cash}"
			end
			allocate_cash
		end

		# Support spot and future.
		def balance_asset(asset, opt={})
			balance() if @balance_cache.nil?
			# Use contract name as asset for future market.
			if market_type() == :future
				asset = contract_name(asset)
			else
				currency, asset = pair_assets(asset) if asset.include?('-')
			end
			bal = @balance_cache.dig(asset, 'cash')||0
			bal += @balance_cache.dig(asset, 'reserved')||0
			bal
		end

		# Support max_order_size(order, opt)
		def max_order_size(pair, type=nil, price=nil, opt={})
			if pair.is_a?(Hash)
				opt = type || {}
				order = pair
				if opt[:use_real_price] == true
					pair, type, price = order['pair'], order['T'], order['p_real']
				else
					pair, type, price = order['pair'], order['T'], order['p']
				end
			else
				# Only support extract p_real from order
				# Do not compute shown price again, not sure maker/taker
				raise "Only support use_real_price with given order." if opt[:use_real_price] == true
			end
			pair = get_active_pair(pair)
			price = format_price_str(pair, type, price, adjust:true, num:true)
			balance = _balance_avail(pair, type, price, opt)
			if ['buy', 'bid'].include?(type.strip.downcase)
				return balance # price is passed into _balance_avail()
				# Not sure maker/taker
				# return (balance/price)*(1.0-rate))
			elsif ['sell', 'ask'].include?(type.strip.downcase)
				return balance
			else
				raise "max_order_size: unknown type #{type}"
			end
		end

		# Callback when balance_cache is really updated in balance_cache_update()
		def balance_changed()
		end

		# Add trade into memory.
		# Upate balance_cache as well.
		# Very complicated if the trade is already in memory.
		def balance_cache_update(trade, opt={})
			raise "trade should contains maker_size" if trade['maker_size'].nil?
			return balance() if @balance_cache.nil?
			# Use an internal function to avoid deadlock in balance()
			return balance_cache_update_int(trade, opt)
		end
		def balance_cache_update_int(trade, opt={})
			reserved_only = opt[:reserved_only] == true
			cancelled = opt[:cancelled] == true
			just_placed = opt[:just_placed] == true
			debug = false # debug = (market_type() == :future)

			# If trade timestamp is earlier than balance initialize time, just remember it.
			# These trades do exist before.
			if parse_trade_time(trade['t']) < @balance_cache_init_time
				# Some exchanges Liqui wont provide reserved amount in return of balance API.
				# It returns zero reserverd value for non-BTC asset,
				# we should recover it from existed orders.
				# Must use flag reserved_only to record them.
				#
				# Some future exchanges Bitmex wont count alive orders into balance API
				# All of its alive orders should be re-computed again.
				# Must use flag recompute to record them.
				#
				# Q: What is the difference between recompute and reserved_only?
				# A: recompute also affect base currency cash, just like the order is just placed.
				#    while reserved_only could only affect underly currency.
				if opt[:recompute] == true
					;
				elsif reserved_only
					;
				else
					return order_remember(trade, verbose:debug)
				end
			end

			old_trade = order_remember(trade, verbose:debug)
			if old_trade != nil # Defensive checking for maker_size
				if just_placed
					raise "just_placed option with an old trade:\n#{format_trade(old_trade)}"
				end
				old_maker_size = old_trade['maker_size']
				# trade from active_orders() and history_orders() has fresh maker_size.
				# This happens in balance() with opt[:recompute] == true
				# Overwrite maker size in this case
				if opt[:recompute] == true && old_maker_size != trade['maker_size']
					puts "Overwrite maker_size #{old_maker_size} for:\n#{format_trade(trade)}"
					trade['maker_size'] = old_maker_size
				end
				if old_maker_size != trade['maker_size']
					puts "trade maker_size changed from #{old_maker_size}:"
					puts format_trade(old_trade)
					puts old_trade.to_json
					puts "trade maker_size changed to: #{trade['maker_size']}"
					puts format_trade(trade)
					puts trade.to_json
					raise "trade maker_size changed."
				end
			end
			puts "update old order:\n#{format_trade(old_trade)}" if debug
			if reserved_only && old_trade != nil
				puts "Order should be brandly new for reserved_only, forget it forcely".red
				order_forget(trade)
			end

			@balance_cache_update ||= {}
			# For spot markets:
			# BTC-DNT means use BTC to trade DNT
			# BTC should be frozen for BID order, otherwise DNT should be fronzen.
			#
			# For future markets, dont modify any cash asset, it acts as margin.
			# Compute contract number only.
			# For bid order nothing needs to be frozen,
			# For ask order:
			# BTC-TRX@20190628 means use BTC to trade TRX future
			# BTC-TRX@20190628 should be frozen for ASK order.
			asset1, cash_inc1, reserved_inc1 = nil, 0, 0 # 1 -> BTC
			# For asset2, compute reserved_v instead of reserved for volume based market.
			# vol_inc2, reserved_vol_inc2 for volume based market.
			asset2, cash_inc2, reserved_inc2, vol_inc2, reserved_vol_inc2 = nil, 0, 0, 0, 0 # 2 -> DNT
			vol_based = (quantity_in_orderbook() == :vol)
			asset1, asset2 = pair_assets(trade['pair'])
			if ['bid', 'buy'].include?(trade['T'].downcase)
				if just_placed
					# Count full size for just placed order.
					reserved_inc1 = trade['p'] * trade['s']
					cash_inc1 = 0 - reserved_inc1
					# Process filled part.
					reserved_inc1 -= trade['p'] * (trade['s'] - trade['remained'])
					cash_inc2 += (trade['s'] - trade['remained'])
					if vol_based
						vol_inc2 += (trade['v'] - trade['remained_v'])
					end
				elsif old_trade.nil?
					reserved_inc1 = trade['p'] * trade['remained']
					cash_inc1 = 0 - reserved_inc1 unless reserved_only
				else # old_trade exist, check quantity of changed.
					# decrease reserved_inc1 and increase cash_inc2
					reserved_inc1 -= trade['p'] * (old_trade['remained'] - trade['remained'])
					cash_inc2 += (old_trade['remained'] - trade['remained'])
					if vol_based
						vol_inc2 += (old_trade['remained_v'] - trade['remained_v'])
					end
				end
				# If order is cancelled, cash back the remained.
				if cancelled
					reserved_inc1 -= trade['p'] * trade['remained']
					cash_inc1 += trade['p'] * trade['remained']
				end
			elsif ['ask', 'sell'].include?(trade['T'].downcase)
				if just_placed
					# Count full size for just placed order.
					reserved_inc2 = trade['s']
					cash_inc2 = 0 - reserved_inc2
					if vol_based
						reserved_vol_inc2 = trade['v']
						vol_inc2 = 0 - reserved_vol_inc2
					end
					# Process filled part.
					reserved_inc2 -= (trade['s'] - trade['remained'])
					cash_inc1 += trade['p'] * (trade['s'] - trade['remained'])
					if vol_based
						reserved_vol_inc2 -= (trade['v'] - trade['remained_v'])
					end
				elsif old_trade.nil?
					reserved_inc2 = trade['remained']
					cash_inc2 = 0 - reserved_inc2 unless reserved_only
					if vol_based
						reserved_vol_inc2 = trade['remained_v']
						vol_inc2 = 0 - reserved_vol_inc2 unless reserved_only
					end
				else # old_trade exist, check quantity of changed.
					# decrease reserved_inc2 and increase cash_inc1
					reserved_inc2 -= (old_trade['remained'] - trade['remained'])
					cash_inc1 += trade['p'] * (old_trade['remained'] - trade['remained'])
					if vol_based
						reserved_vol_inc2 -= (old_trade['remained_v'] - trade['remained_v'])
					end
				end
				# If order is cancelled, cash back the remained.
				if cancelled
					reserved_inc2 -= trade['remained']
					cash_inc2 += trade['remained']
					if vol_based
						reserved_vol_inc2 -= trade['remained_v']
						vol_inc2 += trade['remained_v']
					end
				end
			else
				raise "balance_cache_update: unknown trade type: #{trade.to_json}"
			end

			# future order does not consume cash, only freeze margin.
			cash_inc1 = reserved_inc1 = 0 if market_type() == :future

			if cash_inc1 == 0 && reserved_inc1 == 0 && cash_inc2 == 0 && reserved_inc2 == 0
				puts "Nothing changed for balance_cache" if debug
				return @balance_cache
			end
			puts ['balance_cache_asset1', asset1, @balance_cache[asset1]] if debug
			puts ['balance_cache_asset2', asset2, @balance_cache[asset2]] if debug
			if cash_inc1 != 0 && reserved_inc1 != 0
				puts ['balance_cache_update1', asset1, cash_inc1.round(8), reserved_inc1.round(8)] if @verbose
			end
			if cash_inc2 != 0 && reserved_inc2 != 0
				puts ['balance_cache_update2', asset2, cash_inc2.round(8), reserved_inc2.round(8)] if @verbose
			end
			if vol_inc2 != 0 && reserved_vol_inc2 != 0
				puts ['balance_cache_vol_update2', asset2, vol_inc2, reserved_vol_inc2] if @verbose
			end

			# To avoid @balance_cache is set in another thread,
			# modify cache in local copy.
			asset1_cache = @balance_cache[asset1] || {}
			asset2_cache = @balance_cache[asset2] || {}

			asset1_cache['cash'] ||= 0
			asset1_cache['cash'] += cash_inc1
			asset1_cache['reserved'] ||= 0
			asset1_cache['reserved'] += reserved_inc1

			asset2_cache['cash'] ||= 0
			asset2_cache['cash'] += cash_inc2
			asset2_cache['reserved'] ||= 0
			asset2_cache['reserved'] += reserved_inc2
			if vol_based
				asset2_cache['cash_v'] ||= 0
				asset2_cache['cash_v'] += vol_inc2
				asset2_cache['reserved_v'] ||= 0
				asset2_cache['reserved_v'] += reserved_vol_inc2
				# Adjust cash/reserved by volume, if is zero
				asset2_cache['cash'] = 0 if asset2_cache['cash_v'] == 0
				asset2_cache['reserved'] = 0 if asset2_cache['reserved_v'] == 0
			end
			@balance_cache[asset1] = asset1_cache
			@balance_cache[asset2] = asset2_cache

			balance_cache_print if debug
			balance_changed()
			@balance_cache
		end
		thread_safe :balance_cache_update_int
	end

	class NoMarketClient < Exception
	end

	# For multi-market bot/algo who manages all registered market clients.
	# Refresh multi-markets balance/orders at once.
	# Has ability to compute order real prices and shown prices according to 
	# orderbook and specified market.
	module AssetManager
		include URN::MathUtil
		include URN::OrderUtil
		include APD::LockUtil
		def redis
			URN::RedisPool
		end

		############################# Client GET/SET ##########################
		def client_register(client)
			@trade_clients ||= []
			return if @trade_clients.include?(client)
			puts "Add market_client for #{client.market_name}"
			@trade_clients.push client
			client
		end

		# Return registered market client of market name (or given name)
		# Support order as argument.
		def market_client(market, opt={})
			market = market['market'] if market.is_a?(Hash) # Extract market from order.
			@trade_clients ||= []
			client = @trade_clients.select { |c| c.given_name == market }.first
			return client unless client.nil?
			raise NoMarketClient.new("Need market client for [#{market}]") unless opt[:create_on_miss] == true
			puts "Create market client #{market} on the fly"
			client = URN.const_get(market).new(opt[:create_opt]||{})
			client_register client
			return client
		end

		def list_clients
			@trade_clients
		end

		############################# Balance refreshing ##########################
		# When opt[:cache] is true, return cached balance.
		# Otherwise return cached balance in BALANCE_CACHE_EXPIRE time 
		# Or refresh balance when cache expired.
		def balance_all(opt={})
			return @balance_all_cache if @balance_all_cache != nil && (opt[:cache] == true)
			@balance_all_cache = balance_all_int(opt)
			@balance_all_cache
		end
		BALANCE_CACHE_EXPIRE = 90
		def balance_all_int(opt={})
			verbose = @verbose && opt[:verbose] != false
			# Default: false, parallel checking in thread
			# would make new balance not been seen in main thread.
			parallel = (opt[:parallel] == true)
			market_balance_map = {}
			# Load cache balance.
			@trade_clients.each do |client|
				market = client.market_name
				market_balance = nil
				if opt[:skip_cache] != true && opt[:market] != market
					puts "Querying #{market} balance snapshot..." if verbose
					snapshot = endless_retry(sleep:1) { redis.get "URANUS:#{market}:balance_snapshot" }
					if snapshot != nil
						snapshot = JSON.parse snapshot
						timestamp, data = snapshot[0], snapshot[1]
						if opt[:omit] != nil && opt[:omit].include?(market)
							# Only read from snapshot for omit markets, do not query it directly.
							market_balance = data
							client.balance_cache_write market_balance
						else
							timestamp = DateTime.parse timestamp
							time_diff = ((DateTime.now - timestamp)*3600*24).to_i
							puts "#{market} balance snapshot is #{time_diff} seconds ago." if verbose
							if time_diff < BALANCE_CACHE_EXPIRE
								market_balance = data
								client.balance_cache_write market_balance
							end
						end
					else
						puts "#{market} balance snapshot is missing".red
					end
				elsif opt[:omit] != nil && opt[:omit].include?(market)
					# No snapshots for omit markets.
					next
				end
				market_balance_map[market] = market_balance
			end
			# Fetch realtime balance in parallel.
			puts "Scanning balance..." if market_balance_map.values.include?(nil) && opt[:silent] != true
			if parallel == false || market_balance_map.to_a.select { |mb| mb[1].nil? }.size <= 1
				market_balance_map = market_balance_map.to_a.map do |mb|
					market, market_balance = mb
					next mb unless market_balance.nil?
					market_balance = market_client(market).balance(verbose:false, allow_fail:true, silent:opt[:silent])
					puts "#{market} balance got in serial." unless opt[:silent] == true
					[market, market_balance]
				end.to_h
			else
				market_balance_map = Parallel.map(market_balance_map.to_a) do |mb|
					market, market_balance = mb
					next mb unless market_balance.nil?
					market_balance = market_client(market).balance(verbose:false, allow_fail:true, silent:opt[:silent])
					puts "#{market} balance got in parallel." unless opt[:silent] == true
					[market, market_balance]
				end.to_h
			end
			# Flush back to cache or load from cache if failure occurred.
			market_balance_map.keys.each do |market|
				market_balance = market_balance_map[market]
				if market_balance.nil?
					puts "Fail to load market balance: #{market}".red
					puts "Load snapshot of market balance: #{market}"
					market_balance = endless_retry(sleep:1) { redis.get "URANUS:#{market}:balance_snapshot" }
					if market_balance.nil?
						puts "No snapshot of market balance: #{market}".red
						next
					end
					market_balance = JSON.parse(market_balance)[1]
					market_balance_map[market] = market_balance
				else
					snapshot = [DateTime.now.to_s, market_balance]
					endless_retry(sleep:1) {
						redis.set "URANUS:#{market}:balance_snapshot", snapshot.to_json
					}
				end
			end

			balance_map = {}
			balance = {}
			market_balance_map.each do |market, market_balance|
				balance_map[market] = {}
				market_balance.each do |asset, res|
					balance_map[market][asset] ||= {}
					balance[asset] ||= {}
					res.each do |type, value|
						next if type != 'cash' && type != 'reserved' && type != 'pending'
						balance_map[market][asset][type] = value
						balance[asset][type] ||= 0
						balance[asset][type] += value
						if asset.include?('@') # Also update balance[XRP] for contract BTC-XRP@20190629
							real_asset = asset.split('@').first.split('-')[1]
							balance[real_asset] ||= {}
							balance[real_asset][type] ||= 0
							balance[real_asset][type] += value
						end
					end
				end
			end
			if opt[:verbose] == true
				puts "#{'Total'.ljust(5)} #{format_num('CASH', 6)} #{format_num('RESERVED', 6)}".blue
				balance.each do |k, v|
					puts "#{k.ljust(5)} #{format_num(v['cash'], 6)} #{format_num(v['reserved'], 6)}".blue
				end
			end
			changed = {}
			unless @last_balance_map.nil?
				balance.each do |k, v|
					changed[k] ||= {}
					need_show = false
					v.each do |type, value|
						changed[k][type] = (balance.dig(k,type)||0.0) - (@last_balance_total.dig(k, type)||0.0)
						need_show = changed[k][type] != 0 if need_show == false
					end
					if need_show
						puts "#{(k+'+').ljust(5)} #{format_num(changed[k]['cash'], 6)} #{format_num(changed[k]['reserved'], 6)}".red if verbose
					end
				end
			end
			total_bal = balance.
				to_a.
				map { |kv| [kv[0], kv[1].values.reduce(:+)] }.
				to_h
			ret = {
				:ttl			=> total_bal,
				:bal			=> balance,
				:chg			=> changed,
				:last_map	=> @last_balance_map,
				:bal_map	=> balance_map
			}
			@last_balance_map = balance_map
			@last_balance_total = balance
			ret
		end
		thread_safe :balance_all_int

		############################# Orders Utility ##########################
		# Refresh all alive orders for multiple markets, accepts array
		# Could return as [market][id] HashMap if opt[:result_in_map] is true
		def refresh_orders(orders, opt={})
			return batch_operate_orders(orders, :query_orders, :query_order, opt)
		end
		# Cancel all alive orders for multiple markets, accepts array
		# Could return as [market][id] HashMap if opt[:result_in_map] is true
		def cancel_orders(orders, opt={})
			return batch_operate_orders(orders, :cancel_orders, :cancel_order, opt)
		end
		def batch_operate_orders(orders, batch_method, single_method, opt={})
			verbose = @verbose && opt[:verbose] != false
			if orders.empty?
				return {} if opt[:result_in_map] == true
				return orders
			end

			if orders.is_a?(Hash) # Single order.
				o = orders
				ret = market_client(o).send(single_method, o['pair'], o, opt)
				return o if opt[:allow_fail] == true && ret.nil?
				return ret
			end

			# If given orders have same market and pair, just call market.batch_method
			if order_same_mkt_pair?(orders)
				old_orders = orders
				orders = market_client(orders.first).
					send(batch_method, orders.first['pair'], orders, opt)
				if opt[:result_in_map] == true
					orders = old_orders if opt[:allow_fail] == true && orders.nil?
					returned_orders = {}
					orders.each do |o|
						returned_orders[o['market']] ||= {}
						returned_orders[o['market']][o['i']] = o
					end
					return returned_orders
				else
					return orders
				end
			end

			# Organize alive orders by market then pair.
			grouped_orders = {}
			ungrouped_orders = []
			orders.each do |o|
				grouped_orders[o['market']] ||= {}
				grouped_orders[o['market']][o['pair']] ||= []
				if order_alive?(o)
					grouped_orders[o['market']][o['pair']].push o
				else
					ungrouped_orders.push o
				end
			end
			returned_orders = {}
			grouped_orders.each do |market, po|
				returned_orders[market] ||= {}
				po.each do |pair, orders|
		 			puts "Querying #{market} #{orders.size} #{pair} orders..." if verbose
					if orders.size == 1
						o = market_client(market).
							send(single_method, pair, orders[0], verbose:false, allow_fail:opt[:allow_fail])
						o ||= orders[0]
						returned_orders[market][o['i']] = o
					else
						new_orders = market_client(market).
							send(batch_method, pair, orders, verbose:false, allow_fail:opt[:allow_fail])
						(new_orders || orders).each do |o|
							returned_orders[market][o['i']] = o
						end
					end
				end
			end
			
			# Keep orders in order.
			orders = orders.map do |o|
				next o unless order_alive?(o)
				next returned_orders[o['market']][o['i']] || o
			end
			orders.each { |o| print "#{format_trade(o)}\n" if verbose }

			if opt[:result_in_map] == true
				ungrouped_orders.each do |o|
					returned_orders[o['market']] ||= {}
					returned_orders[o['market']][o['i']] = o
				end
				return returned_orders
			else
				return orders
			end
		end

		# Create new orders as required.
		# Accept single order and array in batch.
		# Returned array might contain nil if allow_fail is true.
		def place_orders(orders, opt={})
			if orders.is_a?(Hash)
				return market_client(o).place_order(o['pair'], o, opt)
			end
			return orders.map do |o|
				market_client(o).place_order(o['pair'], o, opt)
			end
		end

		############################# Price computing ##########################
		# Estimate real price of this order by its price and commission.
		# Will set order['p_real'] if market is not given or kept same.
		# Support:
		# price_real(price, rate, type)
		# price_real(order)
		# price_real(order, nil, orderbook/deviation_type/maker/taker)
		# price_real(order, market)
		# price_real(order, market, orderbook/deviation_type/maker/taker)
		def price_real(order, market=nil, type=nil)
			# Compatiable with price_real(price, rate, type)
			if order.is_a?(Hash) == false
				price, rate = order, market
				return _compute_real_price(price, rate, type)
			end
			precise = @price_precise || 10
			mkt_client = market_client(order)
			different_market = (market != nil && market != order['market'])
			mkt_client = market_client(market) if different_market == true
			rate = nil
			if type.nil? # Use taker fee instead of legacy rate()
				deviation_type = "taker/#{order['T']}"
				rate = mkt_client.preprocess_deviation(order['pair'], t:deviation_type)
			elsif type.is_a?(Array) # Use orderbook to find deviation_type
				orderbook = type
				maker_taker = is_maker?(order, orderbook) ? 'maker' : 'taker'
				deviation_type = "#{maker_taker}/#{order['T']}"
				rate = mkt_client.preprocess_deviation(order['pair'], t:deviation_type)
			elsif type.is_a?(String)
				deviation_type = type
				deviation_type = "#{type}/#{order['T']}" if type == 'maker' || type == 'taker'
				rate = mkt_client.preprocess_deviation(order['pair'], t:deviation_type)
			else
				raise "Unknown type class #{type.class} #{tppe}"
			end
			p_real = 0
			p = mkt_client.format_price_str order['pair'], order['T'], order['p'], adjust:true, num:true
			if order['T'] == 'buy'
				p_real = (p/(1-rate)).floor(precise)
			elsif order['T'] == 'sell'
				p_real = (p*(1-rate)).ceil(precise)
			else
				raise "Unknown order type #{order}"
			end
			order['p_real'] = p_real if different_market == false
			return p_real
		end

		# Estimate order price with given real price minus commission.
		# Support:
		# price_real_set(p_real, rate, type)
		# price_real_set(order)
		# price_real_set(order, nil, orderbook/deviation_type)
		# price_real_set(order, p_real)
		# price_real_set(order, p_real, orderbook/deviation_type)
		def price_real_set(order, p_real=nil, type=nil, opt={})
			if order.is_a?(Hash) == false
				p_real, rate = order, p_real
				return _compute_shown_price(p_real, rate, type)
			end
			precise = @price_precise || 10
			p_real ||= order['p_real']
			mkt_client = market_client(order)
			rate = nil
			if type.nil?
				rate = mkt_client.fee_rate_real(order['pair'], t:"taker/#{order['T']}")
			elsif type.is_a?(Array)
				orderbook = type
				order['p_real'] = p_real # Use p_real in checking is_maker?()
				maker_taker = is_maker?(order, orderbook, use_real_price:true) ? 'maker' : 'taker'
				deviation_type = "#{maker_taker}/#{order['T']}"
				rate = mkt_client.preprocess_deviation(order['pair'], t:deviation_type)
			elsif type.is_a?(String)
				deviation_type = type
				deviation_type = "#{type}/#{order['T']}" if type == 'maker' || type == 'taker'
				rate = mkt_client.preprocess_deviation(order['pair'], t:deviation_type)
			else
				raise "Unknown type class #{type.class} #{tppe}"
			end
			if order['T'] == 'buy'
				order['p_real'] = p_real.floor(precise)
				order['p'] = (order['p_real']*(1-rate)).floor(precise)
			elsif order['T'] == 'sell'
				order['p_real'] = p_real.ceil(precise)
				order['p'] = (order['p_real']/(1-rate)).ceil(precise)
			else
				raise "Unknown order type #{order}"
			end
			order['p'] = mkt_client.format_price_str order['pair'], order['T'], order['p'], adjust:true, num:true
			order['p']
		end

		def is_maker?(order, orderbook, opt={})
			if opt[:use_real_price] == true
				return is_maker_by_real_price?(order, orderbook)
			else
				return is_maker_by_price?(order, orderbook)
			end
		end

		# Given orderbook and order(with shown price) that will be placed
		# Estimate order would be maker/taker
		def is_maker_by_price?(order, orderbook)
			raise "No orderbook provided" if orderbook.nil?
			bids, asks, t = orderbook
			if order['T'] == 'buy'
				# Assume taker if orderbook is not ready.
				return false if asks.nil? || asks.empty?
				return (order['p'] < asks[0]['p'])
			elsif order['T'] == 'sell'
				# Assume taker if orderbook is not ready.
				return false if bids.nil? || bids.empty?
				return (order['p'] > bids[0]['p'])
			else
				raise "Unexpected order type #{order['T']}"
			end
		end

		# Given orderbook and order(with preferred real price) that will be placed
		# Estimate shown price of order.
		# Example:
		# maker/taker fee: 0.1%/0.3%
		# orderbook/sell: 1000 1001 1002 1003 1004
		# Preferred real price of bid order: 1002.5
		# return type should be maker (at price 999.5)
		def is_maker_by_real_price?(order, orderbook)
			raise "No orderbook provided" if orderbook.nil?
			precise = @price_precise || 10
			bids, asks, t = orderbook
			if order['T'] == 'buy'
				return false if asks.nil? || asks.empty?
				return order['p_real'].floor(precise) < asks[0]['p_take'].floor(precise)
			elsif order['T'] == 'sell'
				return false if bids.nil? || bids.empty?
				return order['p_real'].ceil(precise) > bids[0]['p_take'].floor(precise)
			else
				raise "Unknown order type #{order}"
			end
		end
	end

	# Multi-Market functional methods with asynchronised features.
	# Use Concurrent Classes for multi-thread cases.
	class StandardMarketManager
		include URN::AssetManager
		include APD::LockUtil
		def initialize(markets, listeners=[], opt={})
			@listeners = listeners
			@pending_orders = Concurrent::Hash.new
			@alive_orders = Concurrent::Hash.new
			@dead_orders = Concurrent::Hash.new

			@cancel_jobs = Concurrent::Hash.new

			markets.each do |mkt|
				@pending_orders[mkt] ||= Concurrent::Hash.new
				@alive_orders[mkt] ||= Concurrent::Hash.new
				@dead_orders[mkt] ||= Concurrent::Hash.new
				@cancel_jobs[mkt] ||= Concurrent::Hash.new
			end

			URN::OMSLocalCache.monitor(markets, [self], wait:opt[:wait], pair_prefix:opt[:pair_prefix])
			@refresh_thread = Thread.new(abort_on_exception:true) {
				Thread.current[:name] = "StandardMarketManager.refresh_thread"
				refresh_alive_orders()
			}
			@refresh_thread.priority = -3

			puts "#{self.class} init with markets: #{markets}"
		end

		def add_listener(l)
			puts "Listener added #{l.class.name}"
			@listeners.push l
		end

		def monitor_order(trade)
			if trade['i'].nil?  # Pending orders
				@pending_orders[trade['market']][trade['client_oid']] = trade
			elsif order_alive?(trade)
				@alive_orders[trade['market']][trade['i']] = trade.clone
			else
				@dead_orders[trade['market']][trade['i']] = trade.clone
			end
		end

		def query_order(o)
			market_client(o).query_order(o['pair'], o)
		end
		def query_orders(orders)
			orders.map do |o|
				next market_client(o).query_order(o['pair'], o)
			end
		end

		# Send place order request and return client_oid
		# Make sure order is managed in order_cache before requesting.
		def place_order_async(o, order_cache, opt={})
			client = market_client(o)

			# Assign client_oid and add into @pending_orders before sending requests.
			# Sometimes requests failed too fast before client.place_order_async() returns.
			o['client_oid'] = client_oid = client.generate_client_oid(o)
			@pending_orders[o['market']][client_oid] = o
			# Make sure to put order under managed before request is created.
			order_cache[client_oid] = o

			client.place_order_async(o, opt)
			client_oid
		end

		# Asynchronisely cancel order.
		# If trade is already canceled, return it immediately.
		# If trade is already being canceling, add it to monitor list.
		def cancel_order_async(trade, opt={})
			if trade.is_a?(Array)
				return if trade.empty?
				trade.each { |t| cancel_order_async(t, opt) }
				return
			end

			monitor_order(trade)
			return if order_alive?(trade) == false
			return if order_canceling?(trade)

			market_client(trade).cancel_order_async(trade, opt)
		end

		def on_order_update(new_o)
			new_o = new_o.clone
			mkt, i = new_o['market'], new_o['i']

			# Search o in pending_orders
			o = @pending_orders.dig(mkt, new_o['client_oid'])
			if o != nil
				client_oid = new_o['client_oid']
				puts "mgr.pending_orders get updates #{client_oid}"
				@pending_orders[mkt].delete(client_oid)
				if order_alive?(new_o)
					@alive_orders[mkt][i] = new_o
				else
					@dead_orders[mkt][i] = new_o
				end
				# Notify
				@listeners.each { |l| l.on_place_order_done(client_oid, new_o) }
				return
			end

			# Get order form snapshot
			o = @alive_orders.dig(mkt, i) ||
				@dead_orders.dig(mkt, i) ||
				raise("Order #{new_o} is not under managed")
			# Apply new_o only reasonable. Maybe new_o is not newer.
			return unless order_should_update?(o, new_o)
			# puts "Order update:\n#{format_trade(o)}\n#{format_trade(new_o)}"
			# Organize new order by status.
			if order_alive?(new_o)
				@alive_orders[mkt][i] = new_o
				@dead_orders[mkt].delete(i)
			else
				@alive_orders[mkt].delete(i)
				@dead_orders[mkt][i] = new_o
			end
			# Notify
			@listeners.each { |l| l.on_order_update(new_o) }
		end

		def refresh_alive_orders
			loop do # Avoid iteration in thread
				@alive_orders.keys.each do |mkt|
					next if @alive_orders[mkt].empty?
					c = market_client(mkt)
					@alive_orders[mkt].keys.each do |i|
						o = @alive_orders.dig(mkt, i)
						next if o.nil?
						new_o = c.query_order(o['pair'], o, allow_fail:true)
						next if new_o.nil?
						# check if status changed?
						next unless order_changed?(o, new_o)
						on_order_update(new_o)
					end
				end
				sleep 1
			end
			raise "#{self.class.name()} refresh_alive_orders() quit"
		end

		# ID could be id and client_oid.
		def on_oms_broadcast(mkt, i, json_str)
			o = @alive_orders.dig(mkt, i) || @pending_orders.dig(mkt, i)
			return if o.nil?
			if json_str == 'FAILED' # Notify failure.
				puts "<< OMS/B #{mkt} #{i} #{json_str}".red
				@listeners.each { |l| l.on_place_order_rejected(i) }
				return
			end
			puts "<< OMS/B #{mkt} #{i} #{json_str.size}"
			new_o = market_client(mkt).query_order(o['pair'], o, oms_json: JSON.parse(json_str))
			on_order_update(new_o)
		end
	end

	#############################################################
	# Classified market client error - start.
	#############################################################
	class PlaceOrderFailed < Exception
	end

	class NotEnoughBalance < Exception
	end

	# When special points is not enough.
	class NotEnoughPoints < Exception
	end

	class TradingPairNotExist < Exception
	end

	class OrderMightBePlaced < Exception
	end

	class OrderAlreadyPlaced < Exception
	end

	# Occurred when:
	# get active order that not existing, filled, canceled or expired.
	# cancel not existing order.
	# cancel already filled or expired order.
	class OrderNotExist < Exception
	end

	class OrderNotAlive < Exception
	end

	class OrderArgumentError < Exception
	end

	class ActionDisabled < Exception
	end

	class AddrNotInWhitelist < Exception
	end

	NULL_MKT_RESPONSE = '__URANUS_NULL_MARKET_RESPONSE__'
	#############################################################
	# Classified market client error - end.
	#############################################################

	TradeClient ||= {}
	class TradeClientBase
		include URN::OrderUtil
		include URN::MathUtil
		include URN::BalanceManager
		include URN::Misc
		include APD::LockUtil
		include APD::LogicControl
		include APD::ExpireResult

		SATOSHI = 0.00000001
		CLI_TRADE_MODE = 'ab3'

		def initialize(opt={})
			if ENV['IN_RACK'] == '1' && opt[:http_pool_host] != nil
				# http_pool only supports http proxy.
				# [[host, port]]
				@http_proxy_array = @http_proxy_str.map do |str|
					next nil if str.nil?
					next false if str.start_with?('http://') == false
					host_port = str.split('http://')[1].split(':')
					[host_port[0], host_port[1].to_i]
				end.select { |v| v != false } # Remove those not supported.
				if @http_proxy_array.empty?
					puts "http_pool does not support: #{@http_proxy_str}".red
				else
					@http_lib = :http_pool
					@http_pool_host = opt[:http_pool_host] || raise("No http_pool_host")
					@http_pool_headers = (opt[:http_pool_headers] || {}).merge(:Connection => 'Keep-Alive')
					@http_pool_keepalive_timeout = opt[:http_pool_keepalive_timeout] || raise("No http_pool_keepalive_timeout")
					@http_pool_op_timeout = opt[:http_pool_op_timeout] || raise("No http_pool_op_timeout")
					mapping = {
						:read => :read_timeout,
						:write => :write_timeout,
						:connection => :connect_timeout
					}
					mapping.each { |k1, k2|
						if @http_pool_op_timeout[k1].nil?
							raise "No #{k1} in @http_pool_op_timeout #{@http_pool_op_timeout}"
						end
						@http_pool_op_timeout[k2] = @http_pool_op_timeout[k1] # Map to HTTP required key
					}
					@http_persistent_pool = APD::GreedyConnectionPool.new("#{market_name()}-API", 1, debug:false) {
						# Set proxy in creating persistent connections.
						proxy = @http_proxy_array[0]
						proxy = @http_proxy_array[rand(@http_proxy_array.size)] if @http_proxy_array.size > 1
						# @option [Integer] timeout Keep alive timeout, not TCP timeout.
						# Binance gateway keep-alive timeout is 240s
						if proxy.nil?
							next HTTP.use(:auto_inflate).
								persistent(@http_pool_host, timeout:@http_pool_keepalive_timeout).
								timeout(@http_pool_op_timeout).
								headers(@http_pool_headers)
						else
							next HTTP.use(:auto_inflate).via(*proxy).
								persistent(@http_pool_host, timeout:@http_pool_keepalive_timeout).
								timeout(@http_pool_op_timeout).
								headers(@http_pool_headers)
						end
					}
				end
			end

			@verbose = opt[:verbose] == true
			skip_balance = opt[:skip_balance] == true
			@trade_mode = opt[:trade_mode]
			if @trade_mode.nil?
				# If invoked from URN/common/, assign it with CLI_TRADE_MODE
				dir = File.absolute_path(File.dirname(__FILE__))
				cli_dir = File.absolute_path("#{URN::ROOT}/common/")
				if dir == cli_dir
					@trade_mode = CLI_TRADE_MODE
				else
					puts [dir, cli_dir]
					raise("Need a trade mode")
				end
			end
			if ['no', 'test', 'default', 'ab3', 'bulk'].include?(@trade_mode)
				puts "Initializing #{market_name} mode #{@trade_mode}"
			else
				raise "Unknown trade mode #{@trade_mode.inspect}"
			end
			@initializing = true
			@http_lib ||= :restclient
			@http_proxy_array ||= [nil]

			@sha512_digest = OpenSSL::Digest.new('sha512')
			@sha384_digest = OpenSSL::Digest.new('sha384')
			@sha256_digest = OpenSSL::Digest.new('sha256')
			@sha1_digest = OpenSSL::Digest.new('sha1')
			@md5_digest = OpenSSL::Digest.new('md5')

			subscribe_mkt_status() # Do this before sending API requests.

			balance() unless skip_balance
			@initializing = false
			puts "Initializing #{market_name} - finished" if @verbose

			# Record last operation time. For discarding old market data snapshot.
			@operation_time = DateTime.now - 60/(24.0*3600)

			# Async jobs
			@cancel_jobs = Concurrent::Hash.new
			@cancel_cmd_t = Concurrent::Hash.new

			# Latest placed 10 orders
			@_latest_placed_order_ids = Concurrent::Array.new
			@_latest_placed_order_ids.concat([nil]*10)

			@task_name ||=  $0 # Default: script file
		end

		def redis
			URN::RedisPool
		end

		def mkt_req_post(method, url, time, opt={})
			display_args = opt[:display_args]
			proxy = opt[:proxy]
			err = opt[:err]
			if err != nil
				if err.is_a?(String)
					;
				elsif err.is_a?(RestClient::Exception)
					err_res = err.response.to_s[0..200]
					err = "#{err.message} #{err_res}"
				else
					err = "#{err.class} #{err.message}"
				end
			end
			pub_channel = "URANUS:REQ:stat_channel"
			# Only broadcast data when machine is in rack.
			if ENV['IN_RACK'] == '1'
				begin
					pub_msg = [market_name(), url, time, display_args, err, proxy, @task_name].to_json
				rescue # JSON encoding error:"\xE6" from ASCII-8BIT to UTF-8
					pub_msg = [market_name(), url, time, display_args, err.class.to_s, proxy, @task_name].to_json
				end
				redis.publish(pub_channel, pub_msg)
			end
		end

		def mkt_http_req(method, url, opt={})
			req_t = Time.now
			header = opt[:header] || opt[:headers]
			payload = opt[:payload]
			timeout = opt[:timeout]
			display_args = opt[:display_args]
			silent = opt[:silent] == true
			lib = opt[:force_lib] || @http_lib
			path = URI.parse(url).path
			if lib == :restclient
				proxy = @http_proxy_str[0]
				proxy = @http_proxy_str[rand(@http_proxy_str.size)] if @http_proxy_str.size > 1
				response = nil
				pre_t = ((Time.now - req_t)*1000).round(3)
				puts "--> #{lib} #{proxy} #{method} #{path} #{display_args} #{pre_t} ms", level:2 unless silent
				req_t = Time.now.to_f
				req_e = nil
				begin
					if method == :GET
						response = RestClient::Request.execute method: :get, url:url, headers:header, payload:payload, proxy:proxy, timeout:timeout
					elsif method == :POST
						response = RestClient::Request.execute method: :post, url:url, headers:header, payload:payload, proxy:proxy, timeout:timeout
					elsif method == :DELETE
						response = RestClient::Request.execute method: :delete, url:url, headers:header, payload:payload, proxy:proxy, timeout:timeout
					else
						raise "Unknown http method: #{method}"
					end
				rescue => e
					req_e = e
				end
				req_t = Time.now.to_f - req_t
				unless silent
					req_ms = (req_t*1000).round(3)
					puts "<-- #{lib} #{proxy} #{method} #{path} #{display_args} #{req_ms} ms\n#{(response || "")[0..1023].blue}", level:2
				end
				mkt_req_post(method, url, req_t, err:req_e, display_args: display_args, proxy: proxy)
				raise req_e if req_e != nil
				return [response, proxy]
			elsif lib == :http_pool
				# http_persistent_pool reuses TCP connections so timeout setting is not supported.
				# additional per-connection settings are ignored: timeout, headers
				return @http_persistent_pool.with { |conn|
					http_options = conn.default_options.to_hash
					proxy = http_options['proxy']
					pre_t = ((Time.now - req_t)*1000).round(3)
					puts "--> #{lib} #{method} #{path} #{display_args} #{pre_t} ms", level:4 unless silent
					req_t = Time.now.to_f
					req_e = nil
					if payload.is_a?(Hash) # Convert to CGI string
						payload = payload.to_a.map { |kv| "#{kv[0]}=#{kv[1]}" }.join('&')
					end
					begin
						if method == :GET
							response = conn.get url, body:payload
						elsif method == :POST
							response = conn.post url, body:payload
						elsif method == :DELETE
							response = conn.delete url, body:payload
						elsif method == :PUT
							response = conn.put url, body:payload
						else
							raise "Unknown http method: #{method}"
						end
					rescue => e
						req_e = e
					end
					req_t = Time.now.to_f - req_t
					unless silent
						req_ms = (req_t*1000).round(3)
						response = response.to_s
						puts "<-- #{lib} #{method} #{path} #{display_args} #{req_ms} ms\n#{(response || "")[0..1023].blue}", level:4
					end
					mkt_req_post(method, url, req_t, err:req_e, display_args: display_args, proxy: proxy)
					raise req_e if req_e != nil
					response = response.to_s
					next [response, proxy]
				}
			else
				raise "Unknown http lib: #{lib}"
			end
		end

		# API with cache layers : => Redis => block()
		def redis_cached_call_reset(data_key)
			data_key = "URANUS:CACHE:#{given_name()}:#{data_key}"
			endless_retry { redis.del(data_key) }
		end

		def redis_cached_call(data_key, cache_s)
			raise "API block should be passed" unless block_given?
			data_key = "URANUS:CACHE:#{given_name()}:#{data_key}"
			cache = endless_retry { redis.get(data_key) }
			if cache.nil? || cache.empty?
				res = yield
				return nil if res.nil?
				# Cache in redis for 4 hours
				endless_retry { redis.setex(data_key, cache_s, res.to_json) }
			else
				puts "--> Fetch #{data_key}"
				res = JSON.parse(cache)
			end
			return res
		end

		# A name which could be given, default as market_name()
		def given_name
			@given_name || market_name()
		end
		def set_given_name(n)
			@given_name = n
		end

		def task_name
			@task_name
		end

		def set_task_name(n)
			@task_name = n
		end

		def market_name # Use cache to speed up.
			@__cached_market_name ||= self.class.name.split('::').last
			@__cached_market_name
		end

		def short_name
			MARKET_NAME_ABBRV[market_name()] || market_name()
		end

		# If future market is set as {MarketName}@q
		# It will change pair in local_pair() with pair@q
		# And influence data source choosing in refreshing market data.
		def set_future_mode(expiry_sym)
			raise "Only for future markets." unless market_type() == :future
			expiry_sym = expiry_sym.upcase
			raise "set_future_mode() could be invoked only once" unless @future_expiry_sym.nil?
			if ['@W', '@W2', '@M', '@Q', '@P'].include?(expiry_sym)
				puts "#{self} set future mode: expiry_sym #{expiry_sym}"
				@future_expiry_sym = expiry_sym
			else
				raise "Unknown expiry_sym #{expiry_sym}"
			end
		end

		def market_type
			:spot # spot, future, option
		end

		# TODO
		def enable_trading
			@_enable_trading = true
		end

		# TODO
		def read_only?
			@_enable_trading == true
		end

		def subscribe_mkt_status
			return unless @market_status_listener.nil?

			@mkt_status_cache = Concurrent::Hash.new
			_fetch_status()

			channel = "URANUS:#{market_name()}:-:status_channel"
			@market_status_listener = Thread.new(abort_on_exception:true) {
				name = Thread.current[:name] = "#{market_name()}.status_listener"
				begin
					puts "<< #{name} subscribing #{channel}"
					redis.subscribe(channel) { |on|
						on.subscribe { |chn, num|
							puts "<< #{name} subscribed to #{chn} (#{num} subscriptions)"
							_fetch_status()
						}
						on.message { |chn, msg| # Just fetch remote status again once got msg.
							# puts "<< #{name} msg #{msg}"
							_fetch_status()
						}
						on.unsubscribe { |chn, num|
							raise "Unsubscribed to ##{chn} (#{num} subscriptions)"
						}
					}
				rescue => e
					APD::Logger.error e
				end
			}
			puts "Status listener started for #{market_name()}"
		end

		def _fetch_status
			@mkt_status_cache[:ban] = _fetch_banned_info()
		end

		def _fetch_banned_info
			key = "URANUS:#{market_name()}:banned_info"
			str = endless_retry(sleep:1) { redis.get(key) }
			return nil if str.nil?
			j, time, reason = {}, nil, nil
			if str[0] == '{'
				j = parse_json(str)
				time = j['time']
				reason = j['reason']
			else
				j['time'] = time = str
			end
			j['time'] = DateTime.parse(j['time'])
			puts "<< #{market_name} banned_info got: #{j}"
			return j
		end

		# This is the most frequently called method from redis.
		def banned_util(opt={})
			info = @mkt_status_cache[:ban]
			return nil if info.nil?
			return info['time']
		end

		def banned_reason
			info = @mkt_status_cache[:ban]
			return nil if info.nil?
			reason = info['reason']
			if reason != nil && reason.include?('<') && reason.size > 512
				reason = "#{reason[0..50]}...#{reason.size} B HTML"
			end
			return reason
		end

		def set_banned_util(time, reason, opt={})
			key = "URANUS:#{market_name()}:banned_info"
			puts "banned_util -> #{time} #{key}"
			time ||= '19000101'
			value = {
				'time'	=> DateTime.parse(time.to_s),
				'reason'	=> reason
			}
			channel = "URANUS:#{market_name()}:-:status_channel"
			if opt[:broadcast].nil? || (opt[:broadcast] == true)
				# Default: broadcast this
				endless_retry(sleep:1) {
					redis.set(key, value.to_json) # Set key-value
					redis.publish(channel, value.to_json) # Notify other subscribers.
				}
			else # Set locally.
				@mkt_status_cache[:ban] = value
			end
		end

		# Banned info needs to be clear in time.
		# In every bot cycle this info would be parsed.
		def clear_banned(info='')
			@mkt_status_cache.clear
			key = "URANUS:#{market_name()}:banned_info"
			channel = "URANUS:#{market_name()}:-:status_channel"
			puts "clear banned info #{key}"
			endless_retry(sleep:1) {
				redis.del(key)
				redis.publish(channel, "clear_banned #{info}") # Notify other subscribers.
			}
		end

		def is_banned?
			t = banned_util()
			return false if t.nil?
			r = t > DateTime.now
			if r == false
				@mkt_status_cache.clear
				now = Time.now.to_f
				# Print banned into every 5 seconds.
				if @_last_banned_print_t != nil && now - @_last_banned_print_t > 5
					puts "#{market_name()} banned until #{t} :#{banned_reason()}"
				end
				@_last_banned_print_t = now
			end
			r
		end

		def wait_if_banned
			return unless is_banned?()
			t = banned_util()
			now = DateTime.now
			wait_sec = (t - now) * 3600 * 24
			wait_sec = wait_sec.to_i + 1
			puts "Wait until #{t}, #{wait_sec}s", level:2
			keep_sleep(wait_sec)
		end

		# For some exchanges, api rate is restricted at N-req/T-seconds
		# return [N, T] instead.
		def api_rate_limit
			nil
		end

		# Use distribution lock to make sure every request is unde rate limit
		# [N, T] is limit: N reqs in T seconds.
		# Locks are stored as (A,P) where A means a req in N, and P means selected proxy/api_key.
		# res_list could be [proxy] or [api_key]
		def aquire_req_lock(res_list=[nil], opt={})
			@lock_mgr ||= Redlock::Client.new([redis()])
			limit = api_rate_limit()
			return nil if limit.nil?
			max_n, timespan = limit # N reqs in T seconds for each proxy.
			return nil if max_n.nil? || max_n <= 0
			return nil if timespan.nil? || timespan <= 0
			start_t = Time.now.to_f
			t = (timespan*1000).ceil # ms
			# Shuffle target (A,P) pair locks.
			valid_lock, valid_res = nil, nil
			ct = 1
			ttl_choices = res_list.size * max_n
			valid_a, valid_b = nil, nil
			# Shuffle from :
			# [0, res0], [1, res0], [N-1, res0]
			# [0, res1], [1, res1], [N-1, res1]
			# ...
			loop do
				# Choose [a, res_b]
				i = Random.rand(ttl_choices)
				a = i % max_n
				b = i / max_n
				key = "URANUS:LOCK:#{market_name()}_aquire_req:res#{b}_#{a}"
				begin
					lock = @lock_mgr.lock(key, t) # Unlock after timespan.
				rescue Redlock::LockError => e
					puts "#{market_name()} aquire_req_lock error:"
					APD::Logger.highlight e
				end
				if lock.is_a?(Hash)
					valid_lock = lock
					valid_res = res_list[b]
					valid_a = a
					valid_b = b
					break
				else # Failed in aquiring lock
					if ct > ttl_choices && opt[:allow_fail] == true
						puts "#{market_name()} aquire_req_lock failed after round #{ct}"
						return nil
					end
					ct += 1
				end
				puts "#{market_name()} aquire_req_lock wait for round #{ct}"
				keep_sleep 0.1 if ct % ttl_choices
			end
			lock_t = ((Time.now.to_f - start_t)*1000).round(2)
			puts "#{market_name()} aquire_req_lock #{valid_lock[:resource]} in \##{ct} #{lock_t}ms, hold for #{valid_lock[:validity]}ms"
			return [valid_lock, valid_res, valid_b]
		end

		def unlock_req_lock(lock)
			return if lock.nil?
			no_complain {
				lock_t = Time.now.to_f
				ret = @lock_mgr.unlock(lock)
				lock_t = ((Time.now.to_f - lock_t)*1000).round(2)
				puts "#{market_name()} unlock #{lock[:resource]} in #{lock_t}ms"
				ret
			}
		end

		def release_req_lock(lock, opt={})
			return unless lock.is_a?(Hash)
			lock.unlock()
		end

		def is_transferable?
			false
		end

		def can_deposit?(asset)
			true
		end

		def can_withdraw?(asset)
			true
		end

		def valid_addr?(address)
			raise "Wrong address #{address}" unless address =~ /^[a-zA-Z0-9\-_:]{6,256}$/
			true
		end

		def valid_addr_msg?(msg)
			raise "Wrong mesg:[#{msg}]" unless msg =~ /^[a-zA-Z0-9\-]{4,64}$/
			true
		end

		def deposit_fee(asset, opt={})
			0
		end

		# Cache time for asset deposit/withdraw fee.
		MARKET_ASSET_FEE_CACHE_T = 900
		# Cache time for all_pairs
		MARKET_PAIRS_CACHE_T = 6*3600 - 2000

		def record_operation_time
			@operation_time = DateTime.now
		end

		def last_operation_time
			@operation_time
		end

		# For find the most similar pair from CLI argument
		# Most used in future/option market.
		# Example: at date 20190424
		# Default (auto select if only one contract available)
		# BTC-XRP + Bitmex -> BTC-XRP@20190628 + Bitmex
		# TODO
		# Week:
		# USD-BTC + OKEXf@w -> 
		# 2-Week:
		# USD-BTC + OKEXf@w2 -> 
		# Month:
		# USD-BTC + OKEXf@m ->
		# Quarter:
		# USD-BTC + Bitmex@q -> USD-BTC@20190628 + Bitmex
		# Year:
		# USD-BTC + Bitmex@y -> USD-BTC@20190927 + Bitmex
		# Perpetual:
		# USD-BTC + Bitmex@p -> USD-BTC@NIL + Bitmex
		def determine_pair(pair, opt={})
			return pair unless market_type() == :future
			# Find by prefix.
			pairs = all_pairs().keys.select do |p|
				if p.start_with?(pair)
					puts "#{pair} matches #{p}"
					next true
				end
				next false
			end
			if pairs.empty?
				raise "No matched pairs for #{pair}" if opt[:strict] == true
				puts "No matched pairs for #{pair}"
				return pair
			elsif pairs.size == 1
				return pairs.first
			else
				if opt[:strict] == true
					# Show all choices for selection.
					pairs.each_with_index do |p, i|
						preprocess_deviation_evaluate(p) if opt[:preprocess_deviation_evaluate] == true
					end
					order_maps = {}
					pairs.each_with_index { |p, i| order_maps[p] = active_orders(p) }
					pairs.each_with_index do |p, i|
						# Also show balance for each pair. ( Better to choose Future contracts )
						v = @balance_cache[p] || {}
						orders = order_maps[p]
						puts [
							i, '->', p,
							(orders.empty? ? (' '*5) : order_maps[p].size.to_s.rjust(5)),
							format_num(v['cash'], 8),
							format_num(v['reserved'], 8),
							format_num(v['pending'])
						].join(' ').strip
					end
					ret = get_input prompt:"Input number of target pair"
					ret = ret.to_i
					get_input prompt:"Press enter to confirm #{pairs[ret]}"
					return pairs[ret]
				end
				msg = "Select first matched pair #{pairs.first}"
				return pairs.first
			end
		end

		def remember_contract_alias(name, contract)
			raise "Only for future market" unless market_type() == :future
			@contract_alias ||= {}
			if @contract_alias[name] != nil
				if @contract_alias[name] != contract
					raise "Confilct alias #{@contract_alias} for #{name} #{contract}"
				end
				return
			end
			puts "#{market_name()} remember contract alias #{name} -> #{contract}"
			@contract_alias[name] = contract
		end

		def mkt_lazy_load(pair)
		end

		def get_active_pair(pair)
			mkt_lazy_load(pair)
			if market_type() == :spot
				return spot_pair(pair)
			elsif market_type() == :future
				return contract_name(pair)
			else
				raise "Unknown market_type #{market_type()}"
			end
		end

		def contract_name(pair)
			return nil if pair.nil?
			raise "Only for future market" unless market_type() == :future
			# If pair@expiry is expired, return recent active one.
			if pair.include?('@')
				if all_pairs()[pair].nil?
					return contract_name(pair.split('@').first)
				else
					return pair
				end
			end
			raise "Contract missing for #{pair}" if @contract_alias.nil?
			contract = @contract_alias[pair]
			raise "Contract missing for #{pair}" if contract.nil?
			contract
		end

		def spot_pair(pair)
			return nil if pair.nil?
			raise "Only for spot market" unless market_type() == :spot
			return pair unless pair.include?('@')
			# BTC-XRP@20190628 => BTC-XRP
			pair.split('@').first
		end

		# Fixed fee rate for placing orders in legacy traders: ab/ab2
		# This will be only load once and cached in legacy traders.
		def _fee_rate(pair)
			raise "Should not come here any more"
			pair = get_active_pair(pair)
			fee_rate(pair)
		end

		def order_fee(order, opt={})
			# Defensive checking, should only compute fee for non-alive order.
			omit_size = [order['s']/2000.0, 10*SATOSHI].max
			if opt[:allow_alive] != true && order_alive?(order) && order_full_filled?(order, omit_size:omit_size) == false
				# Allow orders that remain little.
				if order['p'] * order['remained'] <= 100*SATOSHI
					;
				elsif order['pair'] == 'BTC-ETH' && order['p'] * order['remained'] <= 1000*SATOSHI
					; # Tolernce little more for ETH
				else
					raise "Order is still alive:\n#{JSON.pretty_generate(order)}"
				end
			end
			return 0 if order['executed'] == 0
			taker_part = order['executed']
			if order['maker_size'] != nil
				taker_part = order['s'] - order['maker_size']
			end
			maker_part = order['executed'] - taker_part
			type = order['T']
			pair = order['pair']
			fee, ft, fm = 0, 0, 0
			# Use saved fee data if order['_data']['fee'] is included when order is created.
			# It is in write_custom_data()
			fee_map = order.dig('_data', 'fee')
			if fee_map.nil?
				ft += order['p']*taker_part*fee_rate_real(pair, t:"taker/#{type}") if taker_part > 0
				fm += order['p']*maker_part*fee_rate_real(pair, t:"maker/#{type}") if maker_part > 0
			else
				ft += order['p']*taker_part*fee_map["taker/#{type}"] if taker_part > 0
				fm += order['p']*maker_part*fee_map["maker/#{type}"] if maker_part > 0
			end
			fee = ft + fm
			fee
		end

		# Real fee rate for computing PnL report.
		def fee_rate_real(pair, opt={})
			pair = get_active_pair(pair)
			fee_rate_real_evaluate(pair) if @_fee_rate_real.nil? || @_fee_rate_real[pair].nil?
			cache = @_fee_rate_real[pair]
			return cache if opt[:t].nil?
			fee = cache[opt[:t]]
			raise "#{market_name()} No fee_rate_real for type [#{opt[:t]}] #{cache}" if fee.nil?
			fee
		end

		def fee_rate_real_evaluate(pair, opt={})
			pair = get_active_pair(pair)
			@_fee_rate_real ||= {}
			@_fee_rate_real[pair] = {
				'maker/buy' => _fee_rate(pair),
				'maker/sell' => _fee_rate(pair),
				'taker/buy' => _fee_rate(pair),
				'taker/sell' => _fee_rate(pair)
			}
		end

		# Whether use complex maker/taker and buy/sell fee model.
		def support_deviation?(pair)
			pair = get_active_pair(pair)
			false
		end

		def trade_mode
			@trade_mode || raise("No trade mode")
		end

		# Return last evaluated virtual price deviation.
		# If opt[:t] is nil, return full fee map.
		def preprocess_deviation(pair, opt={})
			pair = get_active_pair(pair)
			# Use pesudo deviation if it does not support.
			return fee_rate_real(pair, opt) if support_deviation?(pair) == false
			cache = @_preprocess_deviation
			raise "#{market_name()} Call preprocess_deviation_evaluate(#{pair}) before." if cache.nil?
			cache = cache[pair]
			raise "#{market_name()} Call preprocess_deviation_evaluate(#{pair}) before, all_pairs(): #{JSON.pretty_generate(all_pairs())}" if cache.nil?
			return cache if opt[:t].nil?
			fee = cache[opt[:t]]
			raise "#{market_name()} No preprocess_deviation for type [#{opt[:t]}]" if fee.nil?
			fee
		end

		def high_withdraw_fee_deviation(asset)
			nil
		end

		def off_withdraw_fee_deviation(asset)
			0.09 # 0.05 is not enough for some evil exchanges (Binance)
		end

		# Virtual price deviation used in preprocessing orderbook.
		# Usually for placing new orders in arbitrage mode.
		#
		# Default behavior:
		# as same as fee_rate_real_evaluate()
		# --------------------------------------
		#
		# When @trade_mode == ab3:
		# This method might be time consuming because future market
		# deviation depends on its position.
		# Trader should call this method once a while.
		# (after a balance refreshment) or (an order is placed).
		#
		# Spot market deviation depends on its withdraw fee.
		# Trader should call this method once a while.
		# Manual deviation based on fee_rate_real_evaluate()
		# Add 1% for selling deposit disabled ONLY asset.
		# Add 5% for buying withdraw disabled asset.
		# Add X% for buying high withdraw fee asset.
		# --------------------------------------
		#
		def preprocess_deviation_evaluate(pair, opt={})
			pair = get_active_pair(pair)
			map = fee_rate_real_evaluate(pair, opt).clone()
			return nil if map.nil? && opt[:allow_fail] == true
			if trade_mode() != 'ab3'
				@_preprocess_deviation ||= {}
				@_preprocess_deviation[pair] = map
				return
			end
			currency, asset = pair.split('-')
			sell_deviation_adjust = false
			if can_withdraw?(asset) == false
				dev = off_withdraw_fee_deviation(asset)
				map['maker/buy'] += dev
				map['taker/buy'] += dev
			elsif high_withdraw_fee_deviation(asset) != nil
				dev = high_withdraw_fee_deviation(asset)
				map['maker/buy'] += dev
				map['taker/buy'] += dev
			elsif can_deposit?(asset) == false
				# Stop add more deviation when both deposit and withdraw is disabled.
				map['maker/sell'] += 0.01
				map['taker/sell'] += 0.01
				sell_deviation_adjust = true
			end

			if high_withdraw_fee_deviation(currency) != nil && !sell_deviation_adjust
				dev = high_withdraw_fee_deviation(currency)
				# Currency has high withdraw fee, add deviation for selling pair.
				map['maker/sell'] += dev
				map['taker/sell'] += dev
			end

			map = map.to_a.map { |kv| [kv[0], kv[1].round(10)] }.to_h
			@_preprocess_deviation ||= {}
			@_preprocess_deviation[pair] = map
		end

		# next preprocess_deviation_evaluate() result might be different from last result.
		def preprocess_deviation_changed?(pair, opt={})
			pair = get_active_pair(pair)
			false
		end

		def price_step(pair)
			pair = get_active_pair(pair)
			SATOSHI
		end

		def price_precision(pair)
			pair = get_active_pair(pair)
			min_price_step = price_step(pair)
			precision = 0
			loop do
				break if min_price_step >= 1
				min_price_step *= 10
				precision += 1
			end
			precision
		end

		# In orderbook, order quantity in asset/vol (asset by default)
		def quantity_in_orderbook
			:asset
		end

		# if quantity_in_orderbook() is :vol, use vol_step() instead
		def quantity_step(pair)
			pair = get_active_pair(pair)
			SATOSHI
		end
		def vol_step(pair)
			nil
		end

		# Some exchanges has additional minimum volume for single order.
		def min_vol(pair)
			pair = get_active_pair(pair)
			SATOSHI
		end

		def min_quantity(pair)
			pair = get_active_pair(pair)
			SATOSHI
		end

		# Determine smallest order size based on given price.
		def min_order_size(pair, price=nil, opt={})
			# Support min_order_size(order, opt={})
			order, type = nil, nil
			if pair.is_a?(Hash)
				opt = price || {}
				order = pair
				if opt[:use_real_price] == true
					pair, price, type = order['pair'], order['p_real'], order['T']
				else
					pair, price, type = order['pair'], order['p'], order['T']
				end
			else
				# Only support extract p_real from order
				# Do not compute shown price again, not sure maker/taker
				raise "Only support use_real_price with given order." if opt[:use_real_price] == true
			end
			pair = get_active_pair(pair)
			if type != nil
				price = format_price_str(pair, type, price, adjust:true, num:true)
			end
			raise "price invalid #{pair} #{price} #{type} #{order}" if price <= 0
			vol = min_vol(pair)
			s = (vol*10000000000).to_f/(price*10000000000).to_f
			# For those market, order size is shown as volume.
			return s if quantity_in_orderbook() == :vol
			# Format s as integer times of quantity_step()
			step = quantity_step(pair)
			raise "#{market_name} quantity_step(#{pair}) is zero" if step <= 0
			lot = (s/step).ceil
			adjusted_size = lot * step
			[adjusted_size, min_quantity(pair)].max
		end

		# Format order size into integer times of lot.
		# Call format_size_str() internally.
		def format_size(pair, type=nil, size=nil)
			price = nil
			if pair.is_a?(Hash)
				order = pair
				pair, type, size, price = order['pair'], order['T'], order['s'], order['p']
			end
			pair = get_active_pair(pair)
			format_size_str(pair, type, size, price:price, adjust:true, num:true)
		end

		# Format order size into integer times of lot, then apply it into s/v
		# Call format_vol_str()/format_size_str() internally.
		def apply_order_format_size(order)
			pair, type, size, price = order['pair'], order['T'], order['s'], order['p']
			pair = get_active_pair(pair)
			case quantity_in_orderbook()
			when :asset
				s = format_size_str(pair, type, size, price:price, adjust:true, num:true)
				if order['s'] != s
					puts "Format order size #{size} -> #{s}"
					order['s'] = s
				end
			when :vol
				v = format_vol_str(pair, type, price*size, adjust:true, num:true).to_f
				s = v/price
				if order['s'] != s
					puts "Format order size #{size} -> #{s}"
					order['s'] = s
				end
				if order['v'] != v
					puts "Format order vol #{order['v']} -> #{v}"
					order['v'] = v
				end
			else
				raise "Unknown quantity_in_orderbook() in #{market_name()}"
			end
		end

		# Format order size into integer times of lot.
		# For market that show order size in asset, size is in asset and would be formatted.
		#
		# For market that show order size in volume, size is not affected, see format_vol_str()
		# Only if opt[:price] is given, this method would call format_vol_str() then adjust size as well.
		def format_size_str(pair, type, size, opt={})
			pair = get_active_pair(pair)
			verbose = opt[:verbose] == true
			size = size.to_f if size.is_a?(BigDecimal)
			# Must check class to avoid string*bignum
			raise "size should be a num: #{size}" if size.class != 1.class && size.class != (1.1).class
			if quantity_in_orderbook() == :asset
				toint = 10000000000 # 10^11 is precise enough.
				step = quantity_step(pair)
				raise "step should be a num: #{step}" if step.class != 1.class && step.class != (1.1).class
				step_i = (step * toint).round
				size_i = (size * toint).round
				new_size_i = size_i / step_i * step_i
				if new_size_i != size_i
					raise "Size #{format_num(size)} should be integer times of step: #{format_num(step)}" if opt[:adjust] != true
					puts "size #{size_i}->#{new_size_i} stp:#{step_i}" if verbose
					size = new_size_i.to_f/toint.to_f
				end
			elsif quantity_in_orderbook() == :vol && opt[:price] != nil
				price = opt[:price]
				vol = format_vol_str(pair, type, price*size, opt).to_f
				new_size = vol.to_f/price
				raise "Size #{size} should be : #{new_size}" if opt[:adjust] != true if new_size != size
				size = new_size
			end
			return size if opt[:num] == true
			str = format_num(size).strip
			str = str.gsub(/0*$/, '') if str.include?('.')
			str = str.gsub(/\.$/, '') if str.include?('.')
			return str
		end

		# Return true if desired_size matches market lot.
		# Raise error for volume based market.
		def valid_order_size?(pair, type, desired_size, precision, opt={})
			raise "Not for vol based market" if quantity_in_orderbook() != :asset
			actual_size = format_size_str(
				pair, type, desired_size, adjust:true, num:true
			)
			allow_diff = opt[:allow_diff] || 0
			debug = opt[:debug] == true
			if allow_diff == 0
				puts ['valid_order_size', market_name(), desired_size, actual_size, precision] if debug
				return desired_size.round(precision) == actual_size.round(precision)
			else
				df = diff(desired_size, actual_size).abs
				puts ['valid_order_size', market_name(), desired_size, actual_size, df, allow_diff] if debug
				return df <= allow_diff
			end
		end

		# Format order size into integer times of lot.
		# Format order vol into integer times of lot for market that show order size in volume.
		# For market that show order size in asset, size is not affected, see format_vol_str()
		# If opt[:max] is given, vol should  <= opt[:max]
		def format_vol_str(pair, type, vol, opt={})
			pair = get_active_pair(pair)
			verbose = opt[:verbose] == true
			toint = 10000000000 # 10^11 is precise enough.
			size = vol
			# Must check class to avoid string*bignum
			raise "size should be a num: #{size}" if size.class != 1.class && size.class != (1.1).class
			if quantity_in_orderbook() == :vol
				step = vol_step(pair)
				raise "step should be a num: #{step}" if step.class != 1.class && step.class != (1.1).class
				step_i = (step * toint).round
				size_i = (size * toint).round
				# For volume based market, they are all future market.
				# It does not need new_size_i to be floored, round() is better
				new_size_i = (size_i.to_f / step_i.to_f).round * step_i
				if opt[:max] != nil
					max_vol_i = (opt[:max] * toint).round
					max_size_i = (max_vol_i.to_f / step_i.to_f).floor * step_i
					if new_size_i > max_size_i
						new_size_i = max_size_i
						raise "Size #{format_num(size)} should be smaller than max: #{format_num(opt[:max])}" if opt[:adjust] != true
						puts "size #{size_i}->#{new_size_i} max: #{opt[:max]}" if verbose
					end
				end
				if new_size_i != size_i
					raise "Size #{format_num(size)} should be integer times of step: #{format_num(step)}" if opt[:adjust] != true
					puts "size #{size_i}->#{new_size_i} stp:#{step_i}" if verbose
					size = new_size_i.to_f/toint.to_f
				end
				if opt[:in] == :lot # Only return how many lots.
					size = size_i/step_i
				end
			end
			return size if opt[:num] == true
			str = format_num(size).strip
			str = str.gsub(/0*$/, '') if str.include?('.')
			str = str.gsub(/\.$/, '') if str.include?('.')
			return str
		end

		def format_price_str(pair, type, price, opt={})
			pair = get_active_pair(pair)
			verbose = opt[:verbose] == true
			toint = 10000000000 # 10^11 is precise enough.
			step = price_step(pair)
			# Must check class to avoid string*bignum
			raise "#{market_name()} price should be a num: #{price}" if price.class != 1.class && price.class != (1.1).class
			raise "#{market_name()} step should be a num: #{step}" if step.class != 1.class && step.class != (1.1).class
			step_i = (step * toint).round
			price_i = (price * toint).round
			new_price_i = price_i / step_i * step_i
			if new_price_i == price_i
				return price if opt[:num] == true
				str = format_num(price, price_precision(pair)).strip
				str = str.gsub(/0*$/, '') if str.include?('.')
				str = str.gsub(/\.$/, '') if str.include?('.')
				return str
			end
			raise "Price #{format_num(price, price_precision(pair))} should be integer times of step: #{format_num(step, price_precision(pair))} #{new_price_i} #{price_i}" if opt[:adjust] != true
			# Adjust price according to type.
			case type
			when 'buy'
				;
			when 'sell'
				new_price_i += step_i
			else
				raise "Unknown type #{type}"
			end
			puts "#{type} price adjusted from #{price_i} to #{new_price_i} according to step: #{step_i}" if verbose
			return new_price_i.to_f/toint.to_f if opt[:num] == true
			str = format_num(new_price_i.to_f/toint.to_f, price_precision(pair)).strip
			str = str.gsub(/0*$/, '') if str.include?('.')
			str = str.gsub(/\.$/, '') if str.include?('.')
			return str
		end

		def balance_cache_print(opt={})
			head = opt[:head] || 'cache'
			assets = opt[:assets] # Customized filter
			puts "Balance [#{head}] - " + market_name()
			puts "#{'Bal'.ljust(16)} #{format_num('CASH', 8)} #{format_num('RESERVED', 8)} #{format_num('PENDING', 4)}"
			@balance_cache.each do |k, v|
				next if assets != nil && assets.include?(k) == false
				puts "#{(k||'N/A').ljust(16)} #{format_num(v['cash'], 8)} #{format_num(v['reserved'], 8)} #{format_num(v['pending']||0, 4)}"
			end
		end

		def normal_api_error?(e)
			return false if e.nil?
			return true if e.is_a?(SOCKSError) # From Socksify warpped Net::HTTP
			return true if e.is_a?(HTTP::ConnectionError)
			return true if e.is_a?(HTTP::TimeoutError)
			return true if e.is_a?(Zlib::BufError)

			err_msg, err_res = '', ''
			if e.is_a?(RestClient::Exception)
				err_msg, err_res = e.message.to_s, e.response.to_s
			else
				err_msg = e.message.to_s
			end
			return true if err_res.include?('Try again')
			return true if err_msg.include?('Timed out')
			return true if err_msg.include?('Timeout')
			return true if err_msg.include?('Server broke connection')
			return true if err_msg.include?('Internal Server Error')
			return true if err_res.include?('400 Bad Request')
			return true if err_msg.include?('403 Forbidden')
			return true if err_msg.include?('409 Conflict')
			return true if err_msg.include?('429 Too Many Requests')
			return true if err_msg.start_with?('502 ')
			return true if err_msg.include?('502 Bad Gateway')
			return true if err_msg.include?('503 Service Unavailable')
			return true if err_msg.include?('HTTP status code 5')
			return true if err_msg.include?('wrong status line:')
			return true if err_msg.include?('execution expired')
			return false
		end

		# Find any similar orders from recent orders, 
		# Return nil if no similar ordes.
		# Return it if only one similar order.
		# If multiple similar orders exist, cancel them from the latest to oldest,
		# raise OrderMightBePlaced again if more than one are filled, return null if none is filled.
		# 
		# Order args should contains 'pair','T','t'(placed_time_i),'s','p'  For size based orders.
		# Order args should contains 'pair','T','t'(placed_time_i),'v','p'  For volume based orders.
		# Depends on quantity_in_orderbook() is :asset or :vol
		def find_placed_order(recent_orders, order_args, opt={})
			pair, place_time_i, type, price = ['pair','t', 'T', 'p'].map do |k|
				order_args[k] || raise("#{k} missing in #{order_args}")
			end
			size, volume = ['s','v'].map { |k| order_args[k] }
			vol_based = (quantity_in_orderbook() == :vol)
			if vol_based
				raise("v missing in #{order_args}") if volume.nil?
			else
				raise("s missing in #{order_args}") if size.nil?
			end
			pair = get_active_pair(pair)
			dup_orders = recent_orders.
				select { |o| o['pair'] == pair }.
				uniq { |o| o['i'] }.
				sort_by { |o| o['t'].to_i }.reverse.
				select { |o| order_cancelled?(o) == false }.
				select { |o| o['t'] >= place_time_i }.
				select { |o| @_latest_placed_order_ids.include?(o['i']) == false }.
				select { |o| o['T'] == type }.
				select { |o| vol_based ? (o['v'] == volume.to_f) : (o['s'] == size.to_f) }.
				select { |o| o['p'] == price.to_f }
			puts "additional opt: #{opt}"
			if opt[:custom_id_k] != nil && opt[:custom_id_v] != nil
				dup_orders = dup_orders.select { |o|
					print "#{format_trade(o, show:'client_oid')}\n"
					o[opt[:custom_id_k]] == opt[:custom_id_v]
				}
			end
			if false && dup_orders.empty? # DEBUG
				puts order_args.to_json
				recent_orders.each do |o|
					puts format_trade(o)
					puts [o[opt[:custom_id_k]], opt[:custom_id_v]]
					puts [
						o['pair'] == pair,
						order_cancelled?(o) == false,
						o['t'] >= place_time_i,
						@_latest_placed_order_ids.include?(o['i']) == false,
						o['T'] == type,
						vol_based,
						(vol_based ? (o['v'] == volume.to_f) : (o['s'] == size.to_f)),
						o['p'] == price.to_f
					]
					puts o.to_json
				end
				exit
			end
			puts "Similar orders/trade-history:#{dup_orders.size}"
			# Forget those already managed orders.
			dup_orders = dup_orders.select { |o|
				print "#{format_trade(o, show:'client_oid')}\n"
				puts "order managed? #{order_managed?(o)}"
				next order_managed?(o) == false
			}
			if dup_orders.empty?
				puts "No his exists, placing order failed.".blue
				return nil
			end
			if dup_orders.size == 1
				post_place_order(dup_orders[0])
				return dup_orders[0]
			end
			puts "Duplicated orders found, cancel all of them."
			# Cancel them from the oldest to latest, older one has higher priority.
			dup_orders = recent_orders.sort_by { |o| o['i'] }.map do |o|
				next o unless order_alive?(o)
				puts "Cancelling duplicate orders"
				cancel_order(pair, o)
			end
			filled_orders = dup_orders.select{ |o| o['executed'] > 0 }
			puts "Filled duplicated orders:#{filled_orders.size}"
			filled_orders.each { |o| print "#{format_trade(o)}\n" }
			if filled_orders.empty?
				puts "No filled orders, placing order failed.".blue
				return nil
			end
			if filled_orders.size == 1
				post_place_order(filled_orders[0])
				return filled_orders[0]
			end
			puts "Oh no, dup order filled when canceling.".red
			# Print details for debugging.
			filled_orders.each { |o| print "#{format_trade(o)}\n"; puts JSON.pretty_generate(o) }
			raise OrderMightBePlaced.new
		end

		def cancel_canceling_order(pair, order, opt={})
			pair = get_active_pair(pair)
			raise "Order status error #{order}" unless order['status'] == 'canceling'
			puts "Just query canceling order instead of cancel again:" if @verbose
			opt = opt.clone
			loop do
				new_o = query_order(pair, order, opt)
				return nil if new_o.nil? && opt[:allow_fail] == true
				# cancel_order() always copy attributes back to original order.
				# So here we keep this behavior no matter order is canceled or not.
				if order_alive?(new_o)
					new_o.each { |k,v| order[k] = v }
					if oms_enabled?() && opt[:skip_oms] != true && order_age(new_o) >= 10_000
						puts "Maybe dirty OMS cache exists, try query directly again."
						opt[:skip_oms] = true
						next
					end
				else
					post_cancel_order(order, new_o)
					return new_o
				end
				# Order is treat as new, canceling failed.
				return nil if opt[:allow_fail] == true
				sleep 1
			end
		end

		def pre_place_order(pair, order)
			pair = get_active_pair(pair)
			raise "Error trade mode #{trade_mode()}" if trade_mode() == 'no'
			['i', 'client_oid'].each { |k|
				v = order[k]
				raise "Placing order already contains #{k}=#{v}" unless v.nil?
			}
			puts "+++++++++++ PLACE NEW ORDER +++++++++++++++".red, level:2
			# For volume based market, compute and round order volume
			if quantity_in_orderbook() == :vol && order['v'].nil?
				v = order['p']*order['s'].to_f
				order['v'] = format_vol_str(
					pair, order['T'], v,
					adjust:true, verbose:true, num:true
				)
				puts "Volume is set to be #{order['v']}"
			elsif quantity_in_orderbook() == :asset && order['s'].nil?
				order['s'] = order['v']/order['p'].to_f
				puts "Size is set to be #{order['s']}"
			end
			puts "Place #{pair} order: #{format_order(order)}", level:2
			remove_custom_data(order)
			order
		end

		# This method would return new order's client_oid
		def place_order_async(order, opt={})
			opt = opt.clone
			client_oid = order['client_oid']
			if client_oid.nil?
				puts "Suggest to assign an order client_oid before place_order_async()".red
				puts "Sometimes requests failed too fast before client.place_order_async() returns."
				order['client_oid'] = client_oid = generate_client_oid(order)
			end
			
			if URN::MarketAgent.support?(order['market'])
				puts "place_order_async [#{order['market']}] [#{client_oid}] -> remote\n".blue + format_trade(order)
				cmd = {
					'method' => 'create',
					'order'  => order,
					'opt'    => opt
				}.to_json
				URN::MarketAgent.send(redis, order['market'], cmd)
				return client_oid
			end

			puts "place_order_async [#{order['market']}] [#{client_oid}] spawning\n".blue + format_trade(order)
			future = URN.async(name: "place_order_async #{client_oid}") {
				old_priority = Thread.current.priority
				Thread.current.priority = 3
				begin
					trade = place_order(order['pair'], order, opt)
				ensure
					puts "place_order_async [#{order['market']}] [#{client_oid}] finished".blue
					Thread.current.priority = old_priority
				end
			}
			client_oid
		end

		MIN_CANCEL_INVERVAL_MS = 2000
		def cancel_order_async(order, opt={})
			return if order_alive?(order) == false
			return if order_canceling?(order)
			
			i = order['i']
			if URN::MarketAgent.support?(order['market'])
				now = Time.now.to_f # Frequency control 1req/100ms
				if @cancel_cmd_t[i] != nil
					passed_ms = ((now - @cancel_cmd_t[i])*1000).round(3)
					if passed_ms <= MIN_CANCEL_INVERVAL_MS
						# puts "cancel_order_async #{i} freq control, passed_t: #{passed_ms}ms"
						return
					end
					puts "cancel_order_async #{i} again, passed_t: #{passed_ms}ms"
				end
				@cancel_cmd_t[i] = now
				puts "cancel_order_async [#{order['market']}] [#{i}] -> remote\n".blue + format_trade(order)
				cmd = {
					'method' => 'cancel',
					'order'  => order,
					'opt'    => opt
				}.to_json
				# TODO send() might cost 100ms or more
				URN::MarketAgent.send(redis, order['market'], cmd)
				return
			end

			# Stop to cancel order twice.
			# When jobs is a future, stop doing.
			# When jobs is :pending_spawn, stop doing.
			# When jobs is :completed or :rejected, could cancel it again
			if @cancel_jobs[i].is_a?(Symbol)
				if @cancel_jobs[i] == :pending_spawn
					puts "@cancel_jobs[#{i}] is :pending_spawn already"
					return
				elsif @cancel_jobs[i] == :completed
					# Completed does not mean canceled
					# puts "@cancel_jobs[#{mkt}][#{i}] is :completed already"
					# return
				end
			elsif @cancel_jobs[mkt][i] != nil # Must be Future now
				puts "@cancel_jobs[#{mkt}][#{i}] is running already"
				return
			end
			@cancel_jobs[mkt][i] = :pending_spawn
			puts "cancel_order_async [#{order['market']}] [#{client_oid}] spawning\n".blue + format_trade(order)

			future = URN.async(name: "cancel_order_async #{i}") {
				old_priority = Thread.current.priority
				Thread.current.priority = 3
				begin
					# Cancel until finished.
					canceled_o = market_client(mkt).cancel_order(order['pair'], order)
				ensure
					puts "cancel_order_async [#{order['market']}] [#{i}] finished".blue
					Thread.current.priority = old_priority
				end
				canceled_o
			}

			@cancel_jobs[mkt][i] = future
			@cancel_jobs[mkt][i].add_observer(URN::FutureWatchdog.new() { |time, value, reason|
				if reason.nil? # Log reason.
					@cancel_jobs[mkt][i] = :completed
					puts "@cancel_jobs[#{mkt}][#{i}] finished"
				else
					@cancel_jobs[mkt][i] = :rejected
					puts "@cancel_jobs[#{mkt}][#{i}] failed, #{reason}"
				end
			})
			future
		end

		# For OMS only, might be extended into market clients.
		def account_name
			'-' # Default account in OMS is '-'.
		end

		def oms_enabled?
			false
		end

		# Would print when order info missing.
		def oms_order_info(pair, id_list, opt={})
			if oms_enabled? == false
				print "\r#{market_name()} OMS disabled".red # Warn mildly.
				return nil
			elsif opt[:skip_oms] == true
				puts "Skip #{market_name} OMS".red # Warn mildly.
				return nil
			end
			verbose = @verbose && opt[:verbose] != false
			hash_name = "URANUS:#{market_name()}:#{account_name()}:O:#{pair}"
			puts ">> OMS/#{pair} #{id_list}" if verbose
			if id_list.is_a?(String)
				id = id_list
				if URN::OMSLocalCache.support_mkt?(market_name())
					info = URN::OMSLocalCache.oms_info(market_name(), id)
					if info.nil?
						if opt[:loop].nil?
							puts "<< OMS cache null for #{id}"
						elsif opt[:loop] % 50 == 0
							puts "<< OMS cache null for #{id} \##{opt[:loop]}"
						end
					else
						puts "<< OMS cache #{info.size}" if verbose
						info = JSON.parse(info)
					end
					return info
				end
				t, info = endless_retry(sleep:1) { redis.hmget(hash_name, 't', id) }
				if info.nil?
					puts "<< OMS null for #{id_list}" # if verbose
					return nil
				end
				# Make sure OMS cache contains info, now check the maintained timestamp.
				# OMS should clear t after quit or crashed.
				# If oms_order_write_if_null() done, t might still be nil for rare active pairs.
				# Check if OMS running.
				# Clear old cache if OMS is not running. Maybe OMS crashed without delete status.
				if t.nil?
					oms_running_key = "URANUS:#{market_name()}:#{account_name()}:OMS"
					oms_running = endless_retry(sleep:1) { redis.get(oms_running_key) }
					if oms_running.nil?
						puts "<< OMS no t for #{hash_name}, OMS OFF".red # if verbose
						oms_order_delete(pair, id)
						return nil
					else # Treat info as valid data.
						puts "<< OMS no t for #{hash_name}, OMS #{oms_running}" # if verbose
					end
				end
				puts "<< OMS #{info.size}" if verbose
				return JSON.parse(info)
			elsif id_list.is_a?(Array)
				if URN::OMSLocalCache.support_mkt?(market_name())
					should_print = false
					info_list = id_list.map { |id|
						info = URN::OMSLocalCache.oms_info(market_name(), id)
						should_print = true if info.nil?
						next nil if info.nil?
						next JSON.parse(info)
					}
					puts "<< OMS cache #{id_list} #{info_list.map { |s| s.nil? ? 'N' : s.size }}" if should_print
					return info_list
				end
				args = [hash_name, 't'] + id_list
				info_list = endless_retry(sleep:1) { redis.hmget(*args) }
				t = info_list[0]
				info_list = info_list[1..-1]
				info_all_nil = info_list.all? { |i| i.nil? }
				if info_all_nil
					puts "<< OMS all results empty" # if verbose
					return info_list
				end
				# Make sure OMS cache contains info, now check the maintained timestamp.
				# OMS should clear t after quit or crashed.
				# If oms_order_write_if_null() done, t might still be nil for rare active pairs.
				# Check if OMS running.
				# Clear old cache if OMS is not running. Maybe OMS crashed without delete status.
				if t.nil?
					oms_running_key = "URANUS:#{market_name()}:#{account_name()}:OMS"
					oms_running = endless_retry(sleep:1) { redis.get(oms_running_key) }
					if oms_running.nil?
						puts "<< OMS no t for #{hash_name}, OMS OFF".red # if verbose
						id_list.zip(info_list).each do |id, info|
							oms_order_delete(pair, id) unless info.nil?
						end
						return id_list.map { |s| nil }
					else # Treat info as valid data.
						puts "<< OMS no t for #{hash_name}, OMS ON" if verbose
					end
				end
				should_print = info_list.include?(nil)
				puts "<< OMS #{info_list.map { |s| s.nil? ? 'NULL' : s.size }}" if should_print
				ret = info_list.map { |s| s.nil? ? nil : JSON.parse(s) }
				return ret
			else
				raise "Unknown id_list type #{id_list.class} #{id_list}"
			end
		end

		# For those markets do not support alive orders snapshot only:
		#
		# To speed up query time, fill missed order info with trade (parsed) data.
		# Usually for writing alive order info for next quering.
		# Because touched order always triggers OMS.
		#
		# Caution:
		# If OMS could not clear order timestamp before crash,
		# this would lead to dirty cache for next query.
		#
		# Data is parsed, could be used as trade, no need to parse again.
		#
		# Now use for:
		# Bybit: No open orders snapshot, updates come with full order info.
		# OKEX : No open orders snapshot, updates come with full order info.
		# Binan: No open orders snapshot, updates come with full order info.
		# HBDM : No open orders snapshot, updates come with full order info.
		# Huobi: No open orders snapshot, updates come with full order info. (WSS V2)
		#
		# Polo: No open orders snapshot, updates come with incremental order info.
		#       Polo OMS client should delete old order cache when 
		#       incremental update could not be processed.
		def oms_order_write_if_null(pair, id, trade, opt={})
			if oms_enabled? == false
				puts "OMS disabled".red # Warn mildly.
				return nil
			end
			verbose = @verbose && opt[:verbose] != false
			hash_name = "URANUS:#{market_name()}:#{account_name()}:O:#{pair}"
			trade['_parsed_by_uranus'] = true # Mark to skip next parsing.
			ret = endless_retry(sleep:1) { redis.hsetnx(hash_name, id, trade.to_json) }
			if ret == true || ret == 1
				puts ">> write to OMS/#{pair} #{id}".blue
				if URN::OMSLocalCache.support_mkt?(market_name())
					URN::OMSLocalCache.oms_set_if_null(market_name(), id, trade.to_json)
				end
			end
		end

		def oms_order_delete(pair, id)
			if oms_enabled? == false
				puts "OMS disabled, skip oms_order_delete()".red # Warn mildly.
				return nil
			end
			hash_name = "URANUS:#{market_name()}:#{account_name()}:O:#{pair}"
			ret = endless_retry(sleep:1) { redis.hdel(hash_name, id) }
			if ret == 1
				puts ">> Deleted OMS/#{pair} #{id}".red if ret == 1
				if URN::OMSLocalCache.support_mkt?(market_name())
					URN::OMSLocalCache.oms_delete(market_name(), id)
				end
			end
		end

		# Query all alive orders from OMS
		# this might not be accurate, some exchanges has no wss method to get orders snapshot.
		#
		# Huobi: yes
		# Kraken: yes
		#
		# Bybit: no
		# Binance: no
		# Poloniex: no
		# OKEX: no
		def oms_active_orders(pair, opt={})
			if oms_enabled? == false
				puts "OMS disabled".red # Warn mildly.
				return nil
			end
			return oms_active_orders_int(pair, opt) if pair != nil
			verbose = @verbose && opt[:verbose] != false
			# Get all pairs then all orders
			prefix = "URANUS:#{market_name()}:#{account_name()}:O:"
			hash_names = endless_retry(sleep:1) {
				redis.keys('URANUS*').select { |n| n.start_with?(prefix) }
			}
			alive_orders = []
			hash_names.each do |n|
				p = n.split(':').last
				pair_alive_orders = oms_active_orders_int(p, opt)
				next if pair_alive_orders.nil?
				pair_alive_orders.each { |o|
					print "#{o['pair']}\n" if verbose && pair.nil?
					print "#{format_trade(o)}\n" if verbose
				}
				alive_orders += pair_alive_orders
			end
			alive_orders
		end

		def oms_active_orders_int(pair, opt)
			raise "Pair must given" if pair.nil?
			verbose = @verbose && opt[:verbose] != false
			hash_name = "URANUS:#{market_name()}:#{account_name()}:O:#{pair}"
			puts ">> OMS/#{pair} ALL alive" if verbose
			order_map = endless_retry(sleep:1) { redis.hgetall(hash_name) }
			t = order_map.delete('t')
			if t.nil?
				puts "<< OMS no valid t for #{hash_name}" if verbose
				return nil
			end
			# uniq() to keep one from client_oid and id
			latest_orders = order_map.values.
				map { |j| _normalize_trade(pair, JSON.parse(j)) }.
				select { |o| order_alive?(o) }
				.uniq { |o| o['i'] }
			puts "<< OMS #{pair} #{latest_orders.size}/#{order_map.size} alive orders" if verbose
			latest_orders.each { |o|
				print "#{format_trade(o)}\n" if verbose
			}
			latest_orders
		end

		# In any cases, returned json from OMS would contains avg_p for price.
		# even query_order(just_placed:true) would have this problem, need to fix this.
		# Target market: Huobi
		def oms_fix_avg_order_price(order, oms_order)
			if oms_enabled? == false
				puts "OMS disabled".red # Warn mildly.
				return oms_order
			end
			desired_p = order['p']
			if order['p'] != nil && oms_order['p'] != desired_p
				if oms_order['T'] == 'buy' && oms_order['p'] < order['p']
					oms_order['avg_price'] = oms_order['p']
					oms_order['p'] = order['p']
					puts "Trade desired price recovered #{oms_order['p']}"
					puts "Trade has better avg price #{oms_order['avg_price']}"
				elsif oms_order['T'] == 'sell' && oms_order['p'] > order['p']
					oms_order['avg_price'] = oms_order['p']
					oms_order['p'] = order['p']
					puts "Trade desired price recovered #{oms_order['p']}"
					puts "Trade has better avg price #{oms_order['avg_price']}"
				else
					if order['market'] == 'Huobi'
						# When order is parsed from Huobi OMS, its price might be avg price
						# And could be higher than placed price shown from REST API.
						# We overwrite price here.
						if oms_order['p'] != order['p'] && diff(oms_order['p'], order['p']) < 0.02
							oms_order['avg_price'] = oms_order['p']
							oms_order['p'] = order['p']
							puts "Huobi OMS order shown price recovered #{oms_order['p']}"
							puts "It has better avg price #{oms_order['avg_price']}"
							return oms_order
						end
					end
					puts order.to_json
					puts oms_order.to_json
					raise "Trade price should not be worse than desired order"
				end
			end
			oms_order
		end

		# Wait OMS data matches mode in max_time
		# If data does not appear, return nil
		# Otherwise return data.
		#
		# When opt[:force_update] is true,
		# If no required order appearred in OMS, force update OMS with query_order()
		#
		# This method would return latest trade even no matched found.
		def oms_wait_trade(pair, id, max_time, mode, opt={})
			trade = nil
			loop_start_t = Time.now.to_f
			wait_ct = 0

			oms_cache_supported = URN::OMSLocalCache.support_mkt?(market_name())
			URN::OMSLocalCache.add_listener(Thread.current) if oms_cache_supported
			puts "#{market_name()} oms_cache_supported #{oms_cache_supported}"

			elapsed_s = 0
			loop do
				break unless oms_enabled?
				oms_json = oms_order_info(pair, id, verbose:false, loop:wait_ct)
				if oms_json != nil
					oms_json['_from_oms'] = true
					if mode == :new || mode == :query_new
						puts "Order #{id} found \##{wait_ct}"
						trade = _normalize_trade(pair, oms_json)
						return trade
					elsif mode == :cancel
						trade = _normalize_trade(pair, oms_json)
						if order_alive?(trade) == false
							puts "Dead order #{id} found \##{wait_ct}"
							return trade
						end
					end
				end
				wait_ct += 1

				need_check_time = (wait_ct % 50 == 0) || oms_cache_supported
				if need_check_time
					elapsed_s = Time.now.to_f - loop_start_t
					puts "OMS wait #{market_name()} #{pair} #{id} \##{wait_ct} #{(1000*elapsed_s).round}ms"
					if max_time > 0 && elapsed_s > max_time
						puts "OMS wait timeout"
						break
					end
				end

				if oms_cache_supported
					# Future and OMSLocalCache all would wake this up.
					sleep(max_time-elapsed_s)
				else
					sleep 0.001 # Need to check OMS manually.
				end
			end

			# Failed to wait for matched trade.
			# If force_update is set, it would query order from market.
			if opt[:force_update] != nil
				query_o = opt[:force_update]
				raise "force_update o #{o} is not equal to #{id}" unless query_o['i'] == id
				puts "Force update order #{id} again"
				if mode == :new || mode == :query_new
					trade = query_order(pair, query_o, skip_oms:true, just_placed:true, verbose:true, allow_fail:true)
				elsif mode == :cancel
					oms_order_delete(pair, id)
					trade = query_order(pair, query_o, skip_oms:true, verbose:true, allow_fail:true)
					if trade != nil
						oms_order_write_if_null(pair, id, trade)
					end
				end
			end

			# Return trade even not matched
			trade
		end

		# While placing order, call with customized client oid:
		# _async_operate_order(pair, client_oid, :new) { mkt_req() }
		#
		# While cancelling order, call with order id:
		# _async_operate_order(pair, id, :cancel) { mkt_req() }
		#
		# While querying just placed order, call with order id:
		# Better only do this when order is just placed, query direct from OMS
		# is enough in other cases.
		# _async_operate_order(pair, id, :query_new) { mkt_req() }
		#
		# Q: How to distinguish return is from OMS or asynchronised operation?
		# A: JSON from OMS has key '_from_oms' => true
		def _async_operate_order(pair, client_oid, mode, &block)
			raise "Should pass a block" if block.nil?
			raise "Unknown mode #{mode}" if mode != :new && mode != :cancel && mode != :query_new && mode != :query
			json = nil

			# Do first check from OMS, cost 1~3ms
			oms_json = oms_order_info(pair, client_oid, verbose:false)
			if oms_json != nil # Reuqired order appearred in OMS. Just return result.
				oms_json['_from_oms'] = true
				json = oms_json
				if mode == :new || mode == :query_new
					puts "Order #{client_oid} found at round 0"
					return json
				elsif mode == :query
					return json
				elsif mode == :cancel
					trade = _normalize_trade(pair, json.clone) # Dont change json
					if order_alive?(trade) == false
						puts "Dead order #{client_oid} found at round 0"
						return json
					end
				end
			end

			# Purge OMS cache before canceling order.
			# Sometimes order is canceled already but OMS shows it is still alive (dirty)
			# But sometimes redis would postpone execution that later canceled info is purged.
			# Besides, fullfilled future would lead to break the loop.
			# So it is safe to stop deleting order info from OMS.
# 			if mode == :cancel && oms_enabled?
# 				oms_order_delete(pair, client_oid)
# 			end

			future = URN.async({:name => "_async_operate_order #{client_oid} #{mode}"}, &block)
			future.add_observer(URN::FutureWatchdog.new(Thread.current))

			oms_cache_supported = URN::OMSLocalCache.support_mkt?(market_name())
			URN::OMSLocalCache.add_listener(Thread.current) if oms_cache_supported
			puts "#{market_name()} oms_cache_supported #{oms_cache_supported}"

			future_e = nil
			loop_start_t = Time.now.to_f
			wait_ct = 0
			loop do
				# Check future first after sleep time.
				future_state = future.state
				if future_state == :fulfilled
					json = future.value
					elapsed_s = Time.now.to_f - loop_start_t
					puts "Future #{client_oid} fulfilled. #{(1000*elapsed_s).round(1)}ms"
					break
				end
				# Check OMS, cost 1~3ms, extreme mode 3000ms
				redis_t = Time.now
				oms_json = oms_order_info(pair, client_oid, verbose:false, loop:wait_ct)
				redis_t = ((Time.now - redis_t)*1000).round(3)
				if oms_json != nil # order appearred in OMS. Leave future alone.
					oms_json['_from_oms'] = true
					json = oms_json
					if mode == :new || mode == :query_new || mode == :query
						elapsed_s = Time.now.to_f - loop_start_t
						puts "Order #{client_oid} found \##{wait_ct} #{(1000*elapsed_s).round(1)}ms\nfuture:#{future_state} -> #{future.state} redis_t:#{redis_t} ms"
						break
					elsif mode == :cancel
						trade = _normalize_trade(pair, json.clone) # Dont change json
						if order_alive?(trade) == false
							puts "Future #{client_oid} is canceled" if future.cancel()
							puts "Dead order #{client_oid} found \##{wait_ct}"
							break
						end
					end
				end
				# Nothing from OMS, check and handle different future status.
				if future.complete? == false
					wait_ct += 1
					if wait_ct % 50 == 0
						elapsed_s = Time.now.to_f - loop_start_t
						puts "_async #{market_name()} #{pair} #{client_oid} \##{wait_ct} #{(1000*elapsed_s).round(1)}ms"
					end
					if oms_cache_supported
						sleep() # Future and OMSLocalCache all would wake this up.
					else
						sleep 0.001 # Need to check OMS manually.
					end
				else # REST API finished.
					if future.fulfilled?
						json = future.value
						elapsed_s = Time.now.to_f - loop_start_t
						puts "Future #{client_oid} fulfilled. #{(1000*elapsed_s).round(1)}ms"
						break
					elsif future.rejected? # REST API failed
						future_e = future.reason
						puts "Future rejected with #{future_e.class}"
						raise future_e
					else
						raise "Unknown future state #{future.state}"
					end
				end
			end
			json
		end

		# Used in pre_place_order(), to make sure new version of custom data is written.
		def remove_custom_data(trade)
			trade['_data'] = {}
		end

		# Write desired deviation, and real fee_rate into  _data
		def write_custom_data(trade)
			trade['_data'] ||= {}
			trade['_data']['dv'] ||= preprocess_deviation(trade['pair']).clone
			trade['_data']['fee'] ||= fee_rate_real(trade['pair']).clone
		end

		def post_place_order(trade)
			record_operation_time()
			# For some market (Bitmex), balance() needs to be invoked before tradeing.
			# Otherwise deadlock could happen:
			# balance_cache_update() -> balance() -> balance_cache_update() -> ...
			if ['Bitmex'].include?(market_name())
				puts "Call balance() to avoid deadlock."
				balance() if @balance_cache.nil?
			end

			balance_cache_update(trade, just_placed:true)
			write_custom_data(trade)
			puts "#{market_name()} new order #{trade['i']}"
			print "#{format_trade(trade)}\n"
			# Remember latest placed orders.
			@_latest_placed_order_ids.push(trade['i'])
			@_latest_placed_order_ids.shift
			trade
		end

		def pre_cancel_order(order)
			puts "+++++++ #{market_name()} CANCEL ORDER +++++++".red, level:2
			puts "#{format_trade(order)}", level:2
			order
		end

		def post_cancel_order(original_order, canceled_order)
			# canceled_order is sort of queried order too.
			post_query_order(original_order, canceled_order)
			# HitBTC Multi-account Only: Only update balance when account is default.
			if canceled_order['market'] == 'HitBTC' && original_order['account'] != nil
				;
			else
				balance_cache_update(canceled_order, cancelled:true)
			end
			# Copy values back to original order.
			canceled_order.each { |k,v| original_order[k] = v }
			canceled_order
		end

		# 1. Keep order maker_size in new order data.
		# Should be called after query_orders()
		# Should be called after querying existed order. (without just_placed)
		# Should be called after cancelling existed order (with order status returned).
		# Do NOTHING after placing new_o from args:old_o
		#
		# 2. Copy other custom data from old_order into new_order
		# Some status is written into old_order when placing order, which should be kept.
		def post_query_order(old_o, new_o, opt={})
			verbose = opt[:verbose] == true
			puts "post_query_order start" if verbose
			# For some market (Bitmex), balance() needs to be invoked before tradeing.
			# Otherwise deadlock could happen:
			# balance_cache_update() -> balance() -> balance_cache_update() -> ...
			if ['Bitmex'].include?(market_name())
				# puts "Call balance() to avoid deadlock."
				balance() if @balance_cache.nil?
			end

			if opt[:just_placed] == true # If order is just be placed, print and return.
				print "#{format_trade(new_o)}\n" if verbose
				return new_o
			end
			# Custom data complement if order is older than 20190407 162800 +0800
			write_custom_data(new_o) if (new_o['t']||0) < 1554625102130+600_000

			raise "different order:\n#{old_o}\n#{new_o}" unless order_same?(old_o, new_o)
			if old_o == new_o
				print "#{format_trade(new_o)}\n" if verbose
				return new_o
			end
			puts "post_query_order step 1 exchange bug fixing" if verbose
			# Step 1: Exchange bug fixing.
			# Sometimes exchange dirty cache (bittrex) order executed will become less.
			# Overwrite executed with previous value in this case.
			executed_pre = old_o['executed']
			executed_post = new_o['executed']
			if executed_pre != nil && executed_post < executed_pre
				puts "DIRTY_DATA #{market_name()} executed #{executed_post} -> #{executed_pre}"
				# When dirty cache appearred, better delete it, happened in Polo
				oms_order_delete(new_o['pair'], new_o['i']) if oms_enabled?
				# Force forgetting old order in order manager, its maker_size might be bigger.
				# Which could lead to an error.
				order_forget(old_o)
				new_o['executed'] = executed_pre
				new_o['remained'] = new_o['s'] - new_o['executed']
			end
			puts "post_query_order step 2 defensive checking" if verbose
			# Step 2: Defensive checking.
			# Most of them has been checked in order_same?() before
			order, trade = old_o, new_o
			if (order['executed'] || trade['executed']) > trade['executed'] ||
					(order['p'] || trade['p']).round(10) != trade['p'].round(10) ||
				(order['T'] || trade['T']) != trade['T']
					puts order.to_json
					puts trade.to_json
					puts ((order['executed'] || trade['executed']) > trade['executed'])
					puts (order['p'] || trade['p']).round(10) != trade['p'].round(10)
					puts ((order['T'] || trade['T']) != trade['T'])
				raise "Unconsistent order:\n#{format_trade(order)}\n#{format_trade(trade)}"
			end
			# Step 3: Keep max maker_size
			maker_s_1 = old_o['maker_size'] || old_o['remained'] || 0
			maker_s_2 = new_o['maker_size'] || new_o['remained'] || 0
			maker_s = [maker_s_1, maker_s_2].max
			new_o['maker_size'] = old_o['maker_size'] = maker_s
			# Step 4: Copy custom data
			if old_o['_data'].nil?
				old_o['_data'] = new_o['_data']
			elsif new_o['_data'].nil?
				new_o['_data'] = old_o['_data']
			elsif new_o['_data'] == old_o['_data']
				;
			else
				# _data different, overwrite some from old to new.
				# Raise error for other attributes.
				keys = old_o['_data'].keys + new_o['_data'].keys
				keys.uniq.each do |k|
					next if old_o['_data'][k] == new_o['_data'][k]
					if ['dv', 'fee'].include?(k) # Keep desired deviation and fee
						new_o['_data'][k] = old_o['_data'][k]
					else
						raise "_data [#{k}] are different:\n#{old_o}\n#{new_o}"
					end
				end
			end
			# Step 5: update balance cache.
			puts "post_query_order step 5 update balance" if verbose
			balance_cache_update(new_o)
			print "#{format_trade(new_o)}\n" if verbose
			puts "post_query_order end" if verbose
			new_o
		end

		# Query orders in batch.
		def query_orders(pair, orders, opt={})
			return nil if orders.nil?
			return [] if orders.empty?
			pair = get_active_pair(pair)
			verbose = @verbose && opt[:verbose] != false
			alive_orders = []
			# Query them all directly from OMS when enabled.
			# Dont need to query when OMS is enabled.
			# Don't need to scan alive orders for querying only one order.
			if (oms_enabled? == false) && orders.size > 1
				alive_orders = active_orders(pair, allow_fail:true, verbose:false) || []
			end
			alive_orders = alive_orders.map { |o| [o['i'], o] }.to_h
			orders.map do |o|
				trade = alive_orders[o['i']]
				print "#{format_trade(trade)}\n" if trade != nil && verbose
				unless trade.nil?
					post_query_order(o, trade, opt)
					next trade
				end
				new_o = query_order pair, o, opt
				new_o ||= o if opt[:allow_fail] == true
				new_o
			end
		end

		def pre_cancel_orders(orders, opt)
			raise "cancel_orders() must called with allow_fail:true" unless opt[:allow_fail] == true
			puts "+++++++ #{market_name()} CANCEL ORDER LIST +++++++".red, level:2
			orders.each do |o|
				puts "#{format_trade(o)}", level:2
			end
			orders
		end

		# Cancel orders in batch (overwrite this if possible)
		# opt must contains allow_fail:true
		def cancel_orders(pair, orders, opt={})
			pair = get_active_pair(pair)
			pre_cancel_orders(orders, opt)
			trades = orders.map do |o|
				trade = cancel_order(o['pair'], o, opt) || o
				trade
			end
			trades
		end

		def pre_cancel_all_orders(pair, cached_orders, opt)
			raise "cancel_all_orders() must called with allow_fail:true" unless opt[:allow_fail] == true
			pair = get_active_pair(pair)
			puts "+++++++ #{market_name()} CANCEL #{pair} ORDERS+++++++".red, level:2
			cached_orders.each do |o|
				puts "#{format_trade(o)}", level:2
			end
		end

		# Update cached_orders from canceled_orders.
		# canceled_orders might not be canceled really, they are just returned from batch cancelling API
		# So we need check them before executing post_cancel_orders()
		#
		def post_cancel_orders(pair, cached_orders, canceled_orders, opt)
			pair = get_active_pair(pair)
			order_map = canceled_orders.map { |o| [o['i'], o] }.to_h
			# Some cached_orders might be canceled already and will not be returned and needs to be queried.
			trades = cached_orders.map do |o|
				trade = order_map[o['i']] || query_order(pair, o, opt) || o
				if order_alive?(trade) == false # Must keep custom data.
					post_cancel_order(o, trade)
				else
					post_query_order(o, trade, opt)
				end
				trade
			end
			trades
		end

		# Cancel all orders of pair in batch (overwrite this if possible)
		# opt must contains allow_fail:true
		# Also update orders if specified.
		# No custom _data in all orders in results. so it will not return order status.
		def cancel_all_orders(pair, cached_orders=[], opt={})
			pair = get_active_pair(pair)
			pre_cancel_all_orders(pair, cached_orders, opt)
			orders = active_orders(pair, opt)
			return nil if orders.nil? && opt[:allow_fail] == true
			orders = cancel_orders(pair, orders, opt)
			return nil if orders.nil? && opt[:allow_fail] == true
			trades = post_cancel_orders(pair, cached_orders, orders, opt)
			trades
		end

		def test_balance_with_all_orders(pair)
			pair = get_active_pair(pair)
			balance()
			active_orders(pair).each do |o|
				balance_cache_update(o)
			end
			balance_cache_print()
			snapshot1 = @balance_cache[pair].to_json

			balance()
			active_orders(pair).each do |o|
				balance_cache_update(o)
			end
			balance_cache_print()
			snapshot2 = @balance_cache[pair].to_json

			if snapshot1 != snapshot2
				puts JSON.pretty_generate(snapshot1)
				puts JSON.pretty_generate(snapshot2)
				raise "Balance changed."
			end
		end

		def test_balance_computation(pair=nil)
			pair ||= 'BTC-BCH'
			order_args = {'pair'=>pair,'p'=>0.01,'s'=>1,'T'=>'buy'}
			if market_type() == :future
				pair ||= 'BTC-TRX'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.000001,'s'=>2,'T'=>'buy'}
			end
			preprocess_deviation_evaluate(order_args['pair'])

			# Print balance and max_order_size()
			balance()
			['buy', 'sell'].each do |type|
				order_args['T'] = type
				puts "For order: #{order_args}"
				s = max_order_size(order_args)
				puts "Max order size is: #{s}".blue
				s = min_order_size(order_args)
				puts "Min order size is: #{s}".blue
			end

			# Place order then check balance again.
			balance_cache_print()

			order_args['T'] = 'buy'
			order = place_order pair, order_args

			balance_cache_print()

			order_args['T'] = 'sell'
			order_args['p'] = 0.01
			order = place_order pair, order_args

			balance_cache_print()
		end

		def test_trading_process()
			pair = 'BTC-BCH'
			order_args = {'pair'=>pair,'p'=>0.001,'s'=>1,'T'=>'buy'}
			if market_name() == 'HBDM' || market_name() == 'Gemini'
				pair = 'USD-BTC'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>1000.0,'s'=>0.1,'T'=>'buy'}
			elsif market_name() == 'Kraken'
				pair = 'USD-ETH'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>9900.0,'s'=>0.5,'T'=>'sell'}
			elsif market_name() == 'Bybit'
				pair = 'USD-BTC'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>6000.0,'v'=>1,'T'=>'buy'}
			elsif market_name() == 'BNCM'
				pair = 'USD-BTC@P'
				order_args = {'pair'=>pair,'p'=>10000.0,'v'=>100,'T'=>'buy'}
			elsif market_name() == 'Bitmex'
				pair = 'BTC-TRX'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.000001,'s'=>2,'T'=>'buy'}
			elsif ['Polo', 'Binance', 'OKEX'].include?(market_name())
				pair = 'BTC-QTUM'
				order_args = {'pair'=>pair,'p'=>0.0001,'s'=>4,'T'=>'buy'}
			end

			if oms_enabled?()
				URN::OMSLocalCache.monitor(
					[market_name()], [Thread.current], wait:true,
					pair_prefix:order_args['pair']
				)
			end

			preprocess_deviation_evaluate(order_args['pair'])
			orders = []
			data = {"key"=>"value"}
			################### TEST A ###################
			# With test_order_might_be_place flag on/off
			# Place order, cancel it, then cancel it again.
			[false, true].each do |f|
				puts "Test placing order with test_order_might_be_place flag #{f}"
				order = place_order pair, order_args, test_order_might_be_place:f
				orders.push order
				puts format_trade(order)
				id = order['i']
				puts order.to_json
				raise "order must be alive" unless order_alive?(order)
				raise "order id must be string" unless id.is_a?(String)
				raise "order status must be new" unless (order['status'] == 'new' || order_pending?(order))
				raise "order executed must be zero" unless order['executed'] == 0
				sleep 1
				order['_data'] ||= {}
				order['_data']['custom'] = data
				new_o = query_order pair, order
				puts new_o.to_json
				raise "query_order results different" unless order_same?(new_o, order)
				raise "query_order must keep _data" if new_o['_data']['custom'] != data
				canceled_order = cancel_order pair, order
				puts format_trade(canceled_order)
				raise "order must not be alive" if order_alive?(order)
				raise "cancel_order must keep _data" if canceled_order['_data']['custom'] != data
				raise "cancel_order must not be alive" if order_alive?(canceled_order)
				raise "cancel_order id #{canceled_order['i']} != #{id}" unless canceled_order['i'] == id
				raise "order status must be canceled" unless order['status'] == 'canceled'
				raise "order executed must be zero" unless order['executed'] == 0
				puts "TEST: Cancel it again"
				begin
					canceled_order = cancel_order pair, order
				rescue OrderNotExist => e
					puts "OrderNotExist error captured."
				end
				puts format_trade(order)
				raise "order must not be alive" if order_alive?(order)
				raise "cancel_order must keep _data" if canceled_order['_data']['custom'] != data
				raise "cancel_order must not be alive" if order_alive?(canceled_order)
				raise "cancel_order id #{canceled_order['i']} != #{id}" unless canceled_order['i'] == id
				raise "order status must be canceled" unless order['status'] == 'canceled'
				raise "order executed must be zero" unless order['executed'] == 0
			end
			################### TEST B ###################
			# Testing query_orders
			puts "Querying 2 orders"
			new_orders = query_orders pair, orders
			raise "query_orders must return same orders" if new_orders.size != orders.size
			new_orders.each do |o|
				puts format_trade(o)
				puts "data:#{o['_data']}"
				raise "query_orders must keep _data" if o['_data']['custom'] != data
			end
			# Alive order with desired price should be empty.
			10.times.reverse_each { |i|
				orders = active_orders(pair, verbose:true).select { |o| o['p'] == order_args['p'] }
				break if orders.empty?
				puts "Still have alive #{pair} orders left. \##{i}"
				raise "Still have alive #{pair} orders left." if i == 0
				sleep 0.1
			}
			################### TEST C ###################
			# Place orders then cancel_all_orders(pair), then cancel_all_orders() again
			orders = [1,2,3].map do |i|
				o = place_order pair, order_args, tif:'PO'
				o['_data'] ||= {}
				o['_data']['custom'] = data
				o
			end
			loop do
				puts "cancel_all_orders()"
				ret = cancel_all_orders(pair, orders, allow_fail:true)
				if ret != nil
					orders = ret
					orders.each do |o|
						puts "Below order is still alive:\n#{format_trade(o)}\n#{JSON.pretty_generate(o)}" if order_alive?(o)
					end
					break if orders.select { |o| order_alive?(o) }.empty?
				else
					puts "Failed, would try again."
				end
				sleep 3
			end
			orders.each do |canceled_order|
				raise "cancel_order must keep _data" if canceled_order['_data']['custom'] != data
				raise "cancel_order must not be alive" if order_alive?(canceled_order)
				raise "cancel_order status must be canceled" unless canceled_order['status'] == 'canceled'
				raise "cancel_order executed must be zero" unless canceled_order['executed'] == 0
			end
			loop do
				puts "cancel_all_orders() after orders canceled"
				ret = cancel_all_orders(pair, orders, allow_fail:true)
				if ret != nil
					orders = ret
					orders.each do |o|
						puts "Below order is still alive:\n#{format_trade(o)}\n#{JSON.pretty_generate(o)}" if order_alive?(o)
					end
					break if orders.select { |o| order_alive?(o) }.empty?
				else
					puts "Failed, would try again."
				end
				sleep 3
			end
			orders.each do |canceled_order|
				raise "cancel_order must keep _data" if canceled_order['_data']['custom'] != data
				raise "cancel_order must not be alive" if order_alive?(canceled_order)
				raise "cancel_order status must be canceled" unless canceled_order['status'] == 'canceled'
				raise "cancel_order executed must be zero" unless canceled_order['executed'] == 0
			end
			################### TEST D ###################
			# Place orders then cancel_orders(), then cancel_orders() again.
			orders = [1,2,3].map do |i|
				o = place_order pair, order_args
				o['_data'] ||= {}
				o['_data']['custom'] = data
				o
			end
			loop do
				ret = cancel_orders(pair, orders, allow_fail:true)
				if ret != nil
					orders = ret
					break if orders.select { |o| order_alive?(o) }.empty?
				end
				sleep 3
			end
			orders.each do |order|
				# Dont need to query again.
				raise "order must not be alive" if order_alive?(order)
				raise "order must keep _data" if order['_data']['custom'] != data
				raise "order status must be canceled" unless order['status'] == 'canceled'
				raise "order executed must be zero" unless order['executed'] == 0
			end
			loop do
				ret = cancel_orders(pair, orders, allow_fail:true)
				if ret != nil
					orders = ret
					break if orders.select { |o| order_alive?(o) }.empty?
				end
				sleep 3
			end
			orders.each do |order|
				# Dont need to query again.
				raise "order must not be alive" if order_alive?(order)
				raise "order must keep _data" if order['_data']['custom'] != data
				raise "order status must be canceled" unless order['status'] == 'canceled'
				raise "order executed must be zero" unless order['executed'] == 0
			end
			################### TEST END ###################
			return true
		end

		include URN::CLI
		def run_cli(pair=nil)
			puts "Trade mode switched to ab3, #{ARGV}"
			@trade_mode = 'ab3'
			if ARGV.empty?
				return
			elsif ARGV[0] == 'wsskey'
				# Wait until http pool is created.
				sleep 3 unless @http_persistent_pool.nil?
				# Generate websocket authentication key.
				puts "Websocket key:\n#{wss_key()}"
				return
			elsif ARGV[0] == 'bal'
				balance()
				if ARGV.size > 1
					assets = ARGV[1..-1].map { |a| a.upcase }
					balance_cache_print(assets: assets)
				end
				return
			elsif ARGV[0] == 'balwatch' # Loop querying balance.
				loop {
					balance(verbose:false, silent:true)
					if ARGV.size > 1
						assets = ARGV[1..-1].map { |a| a.upcase }
						balance_cache_print(assets: assets)
					end
					keep_sleep 30
				}
				return
			elsif ARGV[0] == 'margin' || ARGV[0] == 'risk'
				margin_status_desc().each { |l|
					puts l
				}
				return
			elsif ARGV[0] == 'pairs'
				msg = all_pairs().to_a.sort.to_h
				puts JSON.pretty_generate(msg)
				return
			elsif ARGV[0] == 'ban?'
				puts "#{self.class.name}\nbanned?:#{is_banned?()}\nbanned util #{banned_util()}, reason: #{banned_reason()}"
				return
			elsif ARGV[0] == 'ban' && ARGV[1] == 'clear'
				set_banned_util(nil, "N/A")
				return
			elsif ARGV[0] == 'ban' && ARGV.size >= 3
				time = DateTime.parse("#{ARGV[1]}+0800")
				reason = ARGV[2]
				puts "#{self.class.name} set it banned util #{time}, reason: #{reason}"
				set_banned_util(time, reason)
				puts "#{self.class.name} banned util #{banned_util()}, reason: #{banned_reason()}"
				return
			elsif ARGV[0] == 'rate'
				monitor_api_rate()
				return
			elsif ARGV[0] == 'ratemax'
				drop_speed = (ARGV[2] || 1).to_f
				if market_name() == 'Kraken'
					get_input prompt:"Will set #{market_name()} api limit to #{ARGV[1].to_i}, drop_speed #{drop_speed}, press enter"
					api_rate_set_max('manually', new_max: ARGV[1].to_i, drop_speed: drop_speed)
				elsif market_name() == 'Binance'
					get_input prompt:"Will set #{market_name()} api limit to original"
					api_rate_set_max('manually')
				else
					raise "Not implemented"
				end
				return
			elsif ARGV[0] == 't'
				puts 'Fast test()'
				test()
				return
			elsif ARGV[0] == 'test'
				results = []
				[ARGV[1].to_i, 1].max.times do |i|
					puts "############## TEST #{i} ##################"
					start_t = Time.now.to_f
					test_trading_process()
					test_t = (Time.now.to_f - start_t).round(2)
					results.push([i, test_t])
					puts "############## TEST #{i} Finished #{test_t} sec ############"
					sleep 5 if i > 0
				end
				puts "############## TOTAL ##################"
				results.each { |r|
					puts r.map { |c| c.to_s.ljust(8) }.join
				}
				return
			elsif ARGV[0] == 'testbal'
				test_balance_computation()
				return
			elsif ARGV[0] == 'test2'
				loop do
					account_list()
				end
				return
			elsif ARGV[0] == 'test3'
				puts binance_symbol_info('BTC-POLY')
				return
			end

			pair = nil
			asset = ARGV[0].strip.upcase
			if asset.include?('-')
				pair = asset
				asset = pair_assets(pair)[1]
			else
				pair = "BTC-#{asset}"
			end
			pair = determine_pair(pair)
			puts "#{self.class.name} runs in CLI mode, target pair: #{pair||'N/A'}"

			if ARGV.size > 1 && ['addr', 'fundin', 'fundout', 'borrow', 'repay'].include?(ARGV[1]) == false
				preprocess_deviation_evaluate(pair)
			end
		
			# Market query
			if ARGV[0] == 'his'
				orders = history_orders(nil)
				orders.sort_by { |o| o['t'] }.each do |o|
					if market_name == 'Bittrex'
						# Print fee rate for bittrex
						rate = ((o['Commission']||o['CommissionPaid']) / o['Price'] * 100.0).round(5)
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']} \t#{rate}%\n"
					else
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']}\n"
					end
				end
				puts "Totally #{orders.size}"
				return
			elsif pair != nil && ARGV[1] == 'his'
				orders = history_orders(pair)
				orders.sort_by { |o| o['t'] }.each do |o|
					if market_name == 'Bittrex'
						# Print fee rate for bittrex
						rate = ((o['Commission']||o['CommissionPaid']) / o['Price'] * 100.0).round(5)
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']} \t#{rate}%\n"
					else
						print "#{format_trade(o)}\n#{o['i']}\n"
					end
				end
				puts "Totally #{orders.size}"
				return
			elsif ARGV[0] == 'alive'
				active_orders(nil)
				return
			elsif pair != nil && ARGV[1] == 'alive'
				active_orders(pair)
				return
			elsif pair != nil && ['borrow', 'repay', 'fundin', 'fundout'].include?(ARGV[1])
				balance()
				type, amount = ARGV[1], ARGV[2]
				if type == 'fundin'
					type = 'in'
					fund_in_out(asset, ARGV[2].to_f, type, allow_fail:true)
				elsif type == 'fundout'
					type = 'out'
					puts "#{type} #{asset} #{amount}".red
					ret = get_input prompt:"Enter YES to #{type} #{asset} #{amount}".red
					exit if ret != "YES"
					fund_in_out(asset, ARGV[2].to_f, type, allow_fail:true)
				elsif type == 'borrow'
					# Show max borrowable amount.
					puts "Max borrowable amount: #{margin_max_borrowable(asset)}"
					puts "#{type} #{asset} #{amount}".red
					ret = get_input prompt:"Enter YES to #{type} #{asset} #{amount}".red
					exit if ret != "YES"
					borrow_repay(asset, ARGV[2].to_f, type, allow_fail:true)
					# Fundout after borrowed
					ret = get_input prompt:"Enter YES to fundout #{asset} #{amount}".red
					exit if ret != "YES"
					fund_in_out(asset, ARGV[2].to_f, 'out', allow_fail:true)
				elsif type == 'repay'
					borrow_repay(asset, ARGV[2].to_f, type, allow_fail:true)
				end
				balance()
				return
			elsif pair != nil && ARGV[1] == 'rule'
				balance() if market_type() == :future
				pairs = ['BTC', 'ETH', 'USDT', 'USD', 'TRX'].map do |base|
					"#{base}-#{asset}"
				end
				pairs = [pair] if market_type() == :future
				pairs.each do |pair|
					puts pair
					if all_pairs().include?(pair) == false
						puts "Not supported".red
						next
					end
					preprocess_deviation_evaluate(pair)
					puts [pair, "min_vol", min_vol(pair)]
					base = pair_assets(pair)[0]
					if base == 'BTC' && ARGV[2] != nil
						puts [pair, "min_order_size", min_order_size(pair, ARGV[2].to_f)]
					end
					puts [pair, "min_quantity", min_quantity(pair)]
					puts [pair, "buy_min_size", format_size_str(pair, 'buy', min_quantity(pair), adjust:true)]
					puts [pair, "ask_min_size", format_size_str(pair, 'sell', min_quantity(pair), adjust:true)]
					puts [pair, "quantity_step", quantity_step(pair)]
					puts [pair, "price_step", price_step(pair)]
					['maker', 'taker'].each do |mt|
						['buy', 'sell'].each do |side|
							puts [pair, "rate #{mt}/#{side}", preprocess_deviation(pair, t:"#{mt}/#{side}")].to_s.blue
						end
					end
					['buy', 'sell'].each do |side|
						if market_type() == :spot
							order = {'pair'=>pair, 'T'=>side, 'p'=>1.0}
							size = max_order_size(order, verbose:true)
							puts [pair, "max_order_size(#{side}) when price=1", size].to_s.blue
						elsif market_type() == :future
							v = future_available_cash(pair, side, verbose:true)
							puts [pair, "future_available_cash(#{side})", v].to_s.blue
							order = {'pair'=>pair, 'T'=>side, 'p'=>1.0}
							size = max_order_size(order, verbose:true)
							puts [pair, "max_order_size(#{side}) when price=1", size].to_s.blue
						end
					end
				end
				return
			elsif pair != nil && ARGV[1] == 'bal'
				test_balance_with_all_orders(pair)
				return
			elsif pair != nil && ARGV[1] == 'fee'
				puts self.class.name
				fee_rate_real_evaluate(pair)
				puts "Legacy rate_fee:"
				if self.respond_to?(:fee_rate)
					puts JSON.pretty_generate(_fee_rate(pair)).red
				else
					puts "NOT SUPPORT ANYMORE".green
				end
				puts "Real fee map:"
				puts JSON.pretty_generate(fee_rate_real(pair))
				puts "Support deviation?"
				if support_deviation?(pair)
					puts "YES".green
				else
					puts "NO".red
				end
				preprocess_deviation_evaluate(pair)
				puts "Orderbook deviation map:"
				puts JSON.pretty_generate(preprocess_deviation(pair))
				if support_deviation?(pair) == false
					puts "Checking if preprocess_deviation(pair) == fee_rate_real(pair)"
					if preprocess_deviation(pair) == fee_rate_real(pair)
						puts "YES".green
					else
						puts "NO".red
					end
				end
				if is_transferable?
					puts "Deposit ? [#{asset}]: #{can_deposit?(asset)}"
					puts "Deposit fee [#{asset}]: #{deposit_fee(asset)}"
					puts "Withdraw ? [#{asset}]: #{can_withdraw?(asset)}"
					puts "Withdraw fee [#{asset}]: #{withdraw_fee(asset)}"
				else
					puts "Withdraw fee [#{asset}]: NON transferrable.".red
				end
				return
			elsif pair != nil && ARGV[1] == 'addr'
				puts "#{self.class.name} deposit [#{asset}]:\n#{deposit_addr(asset)}"
				return
			elsif pair != nil && ARGV[1] == 'tx'
				transactions(asset, limit:2, watch:true)
				return
			elsif pair != nil && ARGV[1] == 'market'
				if self.respond_to?(:market_summary)
					summary = market_summary(pair)
					puts JSON.pretty_generate(summary)
				else
					puts "Not implemented."
				end
				return
			elsif pair != nil && ARGV.size == 2 && ARGV[1].to_s =~ /^[_\-0-9A-Za-z]{5,64}$/
				order = {'market'=>market_name(), 'pair'=>pair, 'i'=>ARGV[1]}
				if ARGV[1].start_with?('CLIENTOID')
					client_oid = ARGV[1]['CLIENTOID'.size..-1]
					puts "client_oid mode"
					order = {'market'=>market_name(), 'pair'=>pair, 'client_oid'=>client_oid}
				else
					puts "order id mode #{ARGV[1]}"
				end
				loop do
					o = query_order(pair, order)
					puts JSON.pretty_generate(o)
					break unless order_alive?(o)
					sleep 1
				end
				# Also query_order in api V3 for Bittrex.
				query_order_v3(order['i']) if market_name() == 'Bittrex'
				return
			end

			# Market action
			if oms_enabled?()
				URN::OMSLocalCache.monitor([market_name()], [Thread.current], wait:true)
			end
			if pair != nil && ARGV[1] == 'cancel' && ARGV[2] != nil
				id = ARGV[2].strip
				puts "Cancelling #{id}"
				orders = active_orders(pair)
				if orders.empty?
					puts "No alive orders"
				elsif id == 'all'
					input = get_input prompt:"Press Y to cancel all [#{ARGV[3]}] orders"
					abort "Abort canceling order" unless input.downcase.strip == 'y'
					target_orders = orders.select do |t|
						next false unless order_alive?(t)
						# skip small orders.
						if market_type == :spot
							if t['p']*t['remained'] < min_vol(pair)
								puts "skip tiny spot order #{t['i']}:\n#{format_trade(t)}"
								next false
							end
						end
						if ARGV[3].nil?
							next true
						elsif ARGV[3] == 'buy'
							next (t['T'] == 'buy')
						elsif ARGV[3] == 'sell'
							next (t['T'] == 'sell')
						else
							next false
						end
						puts "will cancel #{t['i']}:\n#{format_trade(t)}"
					end
					canceled_orders = []
					loop do
						cancel_orders = cancel_orders(pair, target_orders, allow_fail:true)
						break if cancel_orders != nil && canceled_orders.select{ |o| order_alive?(o) }.empty?
						sleep 3
					end
					canceled_orders.each do |o|
						puts "Canceled #{t['i']}:\n#{format_trade(t)}"
					end
				else
					timeout = (ARGV[3] || '10').to_i
					orders.each do |t|
						affected = t['i'].to_s.end_with?(id)
						affected ||= (id.to_f.to_s == id && t['s'] == id.to_f)
						next unless affected
						puts t['i']
						input = get_input timeout:timeout, prompt:"Press Y to cancel order, will do it automatically after #{timeout}s:\n#{format_trade(t)}\n"
						input ||= 'y'
						next (puts "Abort canceling order,") unless input.downcase.strip == 'y'
						canceled_t = cancel_order pair, t
						puts "Order cancelled #{t['i']}:\n#{format_trade(canceled_t)}"
					end
					puts "Order does not exist." if orders.empty?
				end
			elsif pair != nil && ARGV.size >= 4 && ['buy', 'sell'].include?(ARGV[1])
				# Placing single new order manually.
				order_args = {
					'pair' => pair,
					'T'	=> ARGV[1],
					'p'	=> ARGV[2].to_f,
					's' => ARGV[3].to_f
				}
				order_args['v'] = order_args.delete('s') if quantity_in_orderbook() == :vol
				tif = nil
				tif = 'PO' if ARGV.include?('po')
				loop do
					input = get_input prompt:"Press Y to confirm #{tif} order\n#{format_trade(order_args)}"
					abort "Abort placing order" unless input.downcase.strip == 'y'
					order = place_order pair, order_args, notag:true, tif: tif
					puts JSON.pretty_generate(order)
					puts "Order placed #{order['i']}:\n#{format_trade(order)}"
					# Keep querying order status.
					begin
						loop do
							sleep 2
							order = query_order pair, order, verbose:true
							break unless order_alive?(order)
						end
					rescue SystemExit, Interrupt => e # Interrupt to change order.
						ret = get_input prompt:"Exit/Interrupt caught, cancel this order? #{order['i']}"
						if ret.to_s.strip.upcase == 'Y'
							order = cancel_order(pair, order)
							puts "#{order['i']}:\n#{format_trade(order)}"
						else
							break
						end
						ret = get_input prompt:"Change price for remain size #{order['remained']} ?"
						ret = ret.to_s.strip.upcase
						if ret =~ /^[0-9\.]*$/ # New price received.
							puts "Change price to #{ret}"
							order_args['p'] = ret.to_f
							if quantity_in_orderbook() == :vol
								order_args['v'] = order['remained_v']
							else
								order_args['s'] = order['remained']
							end
							next
						else # Input is not a number
							break
						end
					else # No interrupt, order is not alive anymore.
						break
					end
				end
			elsif pair != nil && ARGV.size >= 4 &&
				['step'].include?(ARGV[1]) &&
				['buy', 'sell'].include?(ARGV[2])
				# step buy/sell start_price end_price step size
				start_price, end_price, step_price, size = ARGV[3..6].map { |s| s.to_f }
				orders = []
				vol, size_ttl = 0, 0
				step_price = 0-step_price if end_price < start_price
				((end_price-start_price).abs/step_price).ceil.times do |i|
					order = {
						'T'	=> ARGV[2],
						'p'	=> start_price+i*step_price,
						's'	=> size
					}
					vol += order['p'] * order['s']
					size_ttl += order['s']
					print "#{format_trade(order)}\n"
					orders.push order
				end
				puts "Vol: #{vol.round(8)} Size_total:#{size_ttl.round(8)}"
				input = get_input prompt:"Press Y to confirm these #{orders.size} orders"
				abort "Abort placing order" unless input.downcase.strip == 'y'
				orders.each do |o|
					o = place_order pair, o
					puts "Order placed #{order['i']}:\n#{format_trade(o)}"
				end
			end
			preprocess_deviation_evaluate(pair)
			# List orders at last by default.
			puts "Active orders: #{pair}"
			active_trades = active_orders pair
			active_trades.each do |o|
				puts o['i']
			end
		end
	end

	# For querying trading pairs in multi-market clients
	module MarketPairUtil

		# Convert pair to spider broadcast pair.
		def local_pair(pair, client)
			return client.bfx_shrink_pair(pair) if client.is_a?(URN::BFX)
			return client.binance_encode_pair(pair) if client.is_a?(URN::Binance)
			return pair if @_local_pair_map.nil?
			return @_local_pair_map.dig(client, pair) || pair
		end

		# Temporary remember local pair
		# For future markets: BTC-XRP => BTC-XRP@Q
		def remember_local_pair(pair, local_pair, client)
			@_local_pair_map ||= {}
			@_local_pair_map[client] ||= {}
			old_pair = @_local_pair_map[client][pair]
			if old_pair != nil
				return if old_pair == local_pair
				raise "Unconsistent local pair #{pair} -> #{old_pair} #{local_pair}"
			end
			puts "Remember local pair #{pair} => #{local_pair} for #{client}"
			@_local_pair_map[client][pair] = local_pair
		end

		def load_all_pairs(market_clients)
			cache = "#{URN::ROOT}/tmp/all_pairs.json"
			markets = market_clients.map { |c| c.market_name }
			mkts_pairs = nil
			if File.file?(cache)
				begin
					mkts_pairs = JSON.parse(File.read(cache))
					if (markets-mkts_pairs.keys).empty?
						# Remove unrequired market data.
						mkts_pairs = mkts_pairs.to_a.
							select { |kv| markets.include?(kv[0]) }.to_h
						return mkts_pairs
					end
				rescue => e
					APD::Logger.error e
					mkts_pairs = nil
				end
			end
			mkts_pairs = market_clients.map do |c|
				valid_pairs = c.all_pairs().select do |pair|
					asset = pair_assets(pair)[1]
					c.can_deposit?(asset) || c.can_withdraw?(asset)
				end
				[c.market_name, valid_pairs]
			end.to_h
			File.open(cache, 'w') do |f|
				f.write(JSON.pretty_generate(mkts_pairs))
			end
			mkts_pairs
		end
	end

	# Useful trading basic algorithm moduless.
	module TradeUtil
		include URN::MathUtil
		include URN::CLI

		# Sort market by score from high to low:
		# if order could be filled immediately, score = size_sum(in_price_orders)
		# if order could not be filled immediately, score = 0 - size_sum(over_price_orders)
		# Support use_real_price mode (with commission).
		# Return the market-score array.
		def choose_best_market(odbk_maps, mkt_clients, order, opt={})
			debug = opt[:debug] == true
			exist_order = opt[:exist_order]
			max_optimize_gap = opt[:max_optimize_gap] || 0.05
			max_error_margin = opt[:max_error_margin] || 0.05
			mkt_stat_map = {}
			use_real_price, real_price = false, nil
			if opt[:use_real_price].class == Integer || opt[:use_real_price].class == Float
				use_real_price = true
				real_price = opt[:use_real_price]
			else
				use_real_price = (opt[:use_real_price] == true) # Compare with real price
			end
			exist_order = order if use_real_price == false && exist_order.nil?
			if debug
				puts "choose in #{odbk_maps.keys} for :\n#{format_order(order)}"
			end
			odbk_maps.keys.each do |mkt|
				client = mkt_clients[mkt]
				p = client.format_price_str @pair, order['T'], order['p'], adjust:true, num:true
				if use_real_price && order['market'] != mkt
					rp = (real_price || price_real(order, mkt, odbk_maps[mkt]))
					ro = order.clone
					ro['market'] = mkt
					p = price_real_set(ro, rp, odbk_maps[mkt])
				end
				# Skip when min_order_size is bigger than order remained.
				if client.min_order_size(@pair, p, type:order['T']) > order['remained']
					if debug
						puts "#{mkt} min o size #{client.min_order_size(@pair, p, type:order['T'])}"
					end
					next
				end
				# Skip when quantity step is large to change order size.
				if diff(order['s'], client.format_size(order)) > max_error_margin
					if debug
						puts "#{mkt} quantity_step #{client.format_size(order)}"
					end
					next
				end
				bids, asks, trades = odbk_maps[mkt]
				if bids.nil? || bids.empty? || asks.nil? || asks.empty?
					puts "#{mkt} data is not ready" if debug
					next
				end
				p = (real_price || price_real(order, mkt, odbk_maps[mkt])) if use_real_price
				balance = mkt_clients[mkt].max_order_size(@pair,	order['T'],	p)
				# Do nothing if balance is low.
				if mkt != order['market'] && balance < order['remained']
					puts "#{mkt} balance #{balance} < remained #{order['remained']}" if debug
					next
				end
				if (order_alive?(exist_order) == false) && balance < order['remained']
					puts "#{mkt} balance #{balance} < remained #{order['remained']}" if debug
					next
				end
				in_price_orders, over_price_orders = [], []
				if mkt.start_with?('Binance') && p < 0.2*asks[0]['p'] # Binance min price is 0.1*MKT_PRICE
					puts "Binance price policy banned #{p}" if debug
					next
				end
				if mkt.start_with?('Binance') && p > 2*bids[0]['p'] # Binance max price is 10*MKT_PRICE
					puts "Binance price policy banned #{p}" if debug
					next
				end
				puts "choose_best_market for #{mkt} #{max_optimize_gap}" if debug
				case order['T']
				when 'buy'
					# Do nothing if price is far away from price.
					if use_real_price
						if mkt != order['market'] && (bids[0]['p_make']-p)/p > max_optimize_gap
							puts "#{mkt} price too far #{[bids[0]['p_make'], p]}" if debug
							next
						end
						in_price_orders = asks.select { |o| o['p_take'] <= p }
						over_price_orders = bids.select { |o| o['p_make'] >= p }
					else
						if mkt != order['market'] && (bids[0]['p']-p)/p > max_optimize_gap
							puts "#{mkt} price too far #{[bids[0]['p'], p]}" if debug
							next
						end
						in_price_orders = asks.select { |o| o['p'] <= p }
						over_price_orders = bids.select { |o| o['p'] >= p }
					end
				when 'sell'
					# Do nothing if price is far away from price.
					if use_real_price
						if mkt != order['market'] && (p-asks[0]['p_make'])/p > max_optimize_gap
							puts "#{mkt} price too far #{[asks[0]['p_make'], p]}" if debug
							next
						end
						in_price_orders = bids.select { |o| o['p_take'] >= p }
						over_price_orders = asks.select { |o| o['p_make'] <= p }
					else
						if mkt != order['market'] && (p-asks[0]['p'])/p > max_optimize_gap
							puts "#{mkt} price too far #{[asks[0]['p'], p]}" if debug
							next
						end
						in_price_orders = bids.select { |o| o['p'] >= p }
						over_price_orders = asks.select { |o| o['p'] <= p }
					end
				else
					raise "Unknow order type:#{order}"
				end
				subtract = false
				if exist_order != nil && mkt == exist_order['market'] && order_alive?(exist_order)
					subtract = over_price_orders.select do |o|
						o['s'] >= exist_order['remained'] && o['p'] == exist_order['p']
					end.size > 0
				end
				size1 = in_price_orders.map { |o| o['s'] }.reduce(:+) || 0
				size2 = over_price_orders.map { |o| o['s'] }.reduce(:+) || 0
				size2 -= order['remained'] if subtract == true
				score = (size1 - size2).ceil
				# puts [mkt, size1, size2, score, p, order, exist_order]
				if score == 0 # Sort market by its liquidity,
					case mkt
					when /^HitBTC/
						score = 0.0001
					when /^OKEX/
						score = 0.0002
					when /^Huobi/
						score = 0.0003
					when /^Kraken/
						score = 0.0004
					when /^Polo/
						score = 0.0005
					when /^Bittrex/
						score = 0.0006
					when /^Binance/
						score = 0.0007
					end
				end
				mkt_stat_map[mkt] = {:bal => balance.ceil, :score => score}
			end
			# Post filter, only works when multiple choices exist.
			if mkt_stat_map.size > 1
				# Skip high positive deviation market.
				mkt_stat_map.keys.each { |mkt|
					client = mkt_clients[mkt]
					dv_map = client.preprocess_deviation(order['pair'])
					dv_type = "maker/#{order['T']}"
					if dv_map[dv_type] > 0.005 # 0.5% is much more than normal fee
						puts "Skip high positive deviation market #{mkt} #{dv_map}" if debug
						next
					end
				}
			end
			return mkt_stat_map.to_a.sort_by { |ms| ms[1][:score] }.reverse
		end

		# Sort markets from largest balance to smallest for given order.
		def choose_big_bal_market(mkt_clients, order, odbk_maps, opt={})
			mkt_stat_map = {}
			mkt_clients.keys.each do |mkt|
				client = mkt_clients[mkt]
				bids, asks, trades = odbk_maps[mkt]
				# Skip when market minimum order size is larger than order remained size.
				next if client.min_order_size(order, opt) > order['remained']
				p = mkt_clients[mkt].format_price_str @pair, order['T'], order['p'], adjust:true, num:true
				# Binance sell min price is 0.1*MKT_PRICE
				# Binance buy max price is 10*MKT_PRICE
				next if mkt.start_with?('Binance') && (asks.nil? || asks[0].nil? || p < 0.2*asks[0]['p'])
				next if mkt.start_with?('Binance') && (bids.nil? || bids[0].nil? || p > 2*bids[0]['p'])
				balance = mkt_clients[mkt].max_order_size(order, opt)
				if opt[:use_real_price] == true
					tmp_order = order.clone
					tmp_order['market'] = mkt
					price_real_set(tmp_order, nil, odbk_maps[mkt]) # Compute shown price.
					balance = mkt_clients[mkt].max_order_size(tmp_order, opt)
				end
				# Do nothing if balance is low.
				next if mkt != order['market'] && balance < order['remained']
				balance += order['remained'] if mkt == order['market'] && order_alive?(order)
				mkt_stat_map[mkt] = {:bal => balance.ceil}
			end
			return mkt_stat_map.to_a.sort_by { |ms| ms[1][:bal] }.reverse
		end

		# Suggest an aggressive order with maximum size and price
		# that wont be far away from the ideal average price.
		# Args/type is for the desired order, which is contrast to orderbook order.
		# No need to consider real price in this function, because it could be computed
		# in desired_o as given argument.
		def aggressive_order(orderbook, desired_o, mkt_client, opt={})
			#  OLD: def aggressive_order(orderbook, price_threshold, type, opt={})
			raise "Old aggressive_order() aborted." unless desired_o.is_a?(Hash)
			price_threshold = desired_o['p']
			type = desired_o['T']
			pair = desired_o['pair']
			vol_omit = mkt_client.min_quantity(pair)
			vol_max = desired_o['s']
			market = desired_o['market']
			if market != mkt_client.market_name()
				raise "Market name is not consistent: #{market}, #{mkt_client.market_name()}"
			end
			min_btc_vol = mkt_client.min_vol(pair)

			return {
				:balance_limited	=> false,
				:vol_min_limited	=> true,
				:vol_max_reached	=> true,
				:logs		=> [[], nil, price_threshold, type, opt, orderbook[0..20]]
			} if vol_max.nil? || vol_max < vol_omit

			return {
				:balance_limited	=> true,
				:vol_min_limited	=> false,
				:vol_max_reached	=> false,
				:logs		=> [[], nil, price_threshold, type, opt, orderbook[0..20]]
			} if mkt_client.max_order_size(desired_o, opt) < vol_omit

			# Maximum diff between avg_price and order price.
			avg_price_max_diff = opt[:avg_price_max_diff] || 0.001

			verbose = opt[:verbose] == true
			remote_debug = opt[:remote_debug] == true
			logs = []

			size_sum, price, avg_price = 0, 0, 0
			vol_max_reached, balance_limited, vol_min_limited = false, false, false
			max_scan_depth = 0
			orderbook.each do |o|
				max_scan_depth += 1
				if (type == 'buy' && o['p'] > price_threshold) ||
					(type == 'sell' && o['p'] < price_threshold)

					puts "Price #{o['p']} exceed threshold #{price_threshold}." if verbose
					logs.push "Price #{o['p']} exceed threshold #{price_threshold}." if remote_debug
					break
				end
				s, p = o['s'], o['p']
				# Scan always stop at this side when max vol is reached.
				if vol_max != nil && s + size_sum > vol_max
					s = vol_max - size_sum
					vol_max_reached = true
					puts "Max vol reached, vol_max=#{vol_max}" if verbose
					logs.push "Max vol reached, vol_max=#{vol_max}" if remote_debug
				end
				max_size = mkt_client.max_order_size(pair, type, p)
				if s + size_sum > max_size
					s = max_size - size_sum
					balance_limited = true
					puts "Balance limited, #{type} max=#{max_size} p=#{p}" if verbose
					logs.push "Balance limited, #{type} max=#{max_size} p=#{p}" if remote_debug
				end
				# Break if next average price is far away from the order size.
				next_avg_price = (avg_price * size_sum + p * s) / (size_sum + s)
				puts "scanning: order #{o['p']} #{o['s']} next_avg:#{next_avg_price.round(8)}" if verbose
				logs.push "scanning: order #{o['p']} #{o['s']} next_avg:#{next_avg_price.round(8)}" if remote_debug
				price_diff = diff(next_avg_price, p).abs
				if price_diff > avg_price_max_diff
					puts "Price diff #{price_diff} exceed #{avg_price_max_diff}." if verbose
					logs.push "Price diff #{price_diff} exceed #{avg_price_max_diff}." if remote_debug
					break
				end
				# Record scan progress.
				size_sum += s
				price, avg_price = p, next_avg_price
				puts "size_sum:#{size_sum.round(3)}, price:#{price.round(8)}, avg:#{avg_price.round(8)}" if verbose
				logs.push "size_sum:#{size_sum.round(3)}, price:#{price.round(8)}, avg:#{avg_price.round(8)}" if remote_debug
				if balance_limited || vol_max_reached
					puts "Break here, limit exceed." if verbose
					logs.push "Break here, limit exceed." if remote_debug
					break
				end
			end
			order = {
				'pair'=>pair,
				'p'	=> price,
				's'	=> size_sum,
				'T'	=> type,
				'market'	=> market,
				'ideal_avg_price'	=> avg_price
			}
			puts "suggest_order: #{order.to_json}" if verbose
			logs.push "suggest_order: #{order.to_json}" if remote_debug

			debug_attrs = [size_sum, vol_omit, min_btc_vol, order['p']*order['s']]
			if size_sum == 0
				order = nil
			elsif size_sum <= vol_omit
				vol_min_limited = true
				order = nil
			elsif min_btc_vol != nil && order['p']*order['s'] <= min_btc_vol
				vol_min_limited = true
				order = nil
			end

			{
				:order	=> order,
				:balance_limited	=> balance_limited,
				:vol_min_limited	=> vol_min_limited,
				:vol_max_reached	=> vol_max_reached,
				:debug_attrs	=> debug_attrs,
				:logs		=> [logs, order, price_threshold, type, opt, orderbook[0..max_scan_depth]]
			}
		end

		# Suggest a pair of orders that could make maximum size of market arbitrage.
		# Make sure orders average wont be far away from the price.
		def aggressive_arbitrage_orders(odbk_bid, odbk_ask, min_price_diff, opt={})
			if opt[:mkt_client_bid].nil? || opt[:mkt_client_ask].nil?
				raise "Legacy arguments aborted."
			end

			# New arguments setting to support using real price.
			# mkt_client_bid is the market client of bid orderbook, where ask order should be placed.
			# mkt_client_ask is the market client of ask orderbook, where bid order should be placed.
			mkt_client_bid = opt[:mkt_client_bid]
			mkt_client_ask = opt[:mkt_client_ask]
			pair = opt[:pair] || raise("No pair specified.")
			market_bid = mkt_client_bid.market_name()
			market_ask = mkt_client_ask.market_name()

			use_real_price = opt[:use_real_price] == true
			vol_max = opt[:vol_max]
			vol_min = opt[:vol_min] || 0
			avg_price_max_diff = opt[:avg_price_max_diff] || 0.001

			verbose = (opt[:verbose] || opt[:debug]) == true
			remote_debug = opt[:remote_debug] == true
			logs = []

			balance_limited = false
			# Scan bids and asks one by one, scan bids first.
			scan_status = {
				'buy'	=> {
					'finished'	=> false,
					'idx'				=> 0,
					'size_sum'	=> 0,
					'price'			=> 0,
					'p_real'		=> 0,
					'avg_price'	=> 0
				},
				'sell'	=> {
					'finished'	=> false,
					'idx'				=> 0, # Next scan id.
					'size_sum'	=> 0,
					'price'			=> 0,
					'p_real'		=> 0,
					'avg_price'	=> 0
				}
			}
			# return nil if orderbook is empty.
			if odbk_bid.nil? || odbk_ask.nil? || odbk_bid.empty? || odbk_ask.empty?
				return {
					:logs=>[logs, scan_status, [], min_price_diff, opt, odbk_bid[0..20], odbk_ask[0..20]]
				}
			end

			# Data cleaning.
			odbk_bid = odbk_bid.select { |o| o['s'] != nil }.select { |o| o['s'] > 0 }
			odbk_ask = odbk_ask.select { |o| o['s'] != nil }.select { |o| o['s'] > 0 }

			scan_buy_now = true
			balance_limited, vol_max_reached, vol_min_limited = false, false, false
			loop_ct = 0
			loop do
				loop_ct += 1
				if loop_ct > 9999
					# Bug #1 triggerred.
					debug_info = [nil, nil, nil, min_price_diff, opt, odbk_bid, odbk_ask]
					file_name = "#{URN::ROOT}/debug_#1_#{DateTime.now.strftime('%Y%m%d_%H%M%S')}.json"
					File.open(file_name, 'w') { |f| f.write(JSON.pretty_generate(debug_info)) }
					puts "Bug 1 triggerred, debug info flushed -> #{file_name}"
					return {
						:logs=>[logs, scan_status, [], min_price_diff, opt, odbk_bid[0..20], odbk_ask[0..20]]
					}
				end
				break if scan_status['buy']['finished'] && scan_status['sell']['finished']
				type = scan_buy_now ? 'buy':'sell'
				opposite_type = scan_buy_now ? 'sell':'buy'
				odbk = scan_buy_now ? odbk_bid : odbk_ask
				idx = scan_status[type]['idx']
				if scan_status[opposite_type]['finished'] &&
					scan_status[type]['size_sum'] > scan_status[opposite_type]['size_sum']

					puts "#{opposite_type} side is already finished, #{type} side size_sum is larger than opposite, all finished." if verbose
					logs.push "#{opposite_type} side is already finished, #{type} side size_sum is larger than opposite, all finished." if remote_debug
					scan_status[type]['finished'] = true
					break
				end

				if scan_status[opposite_type]['finished'] &&
					idx >= odbk.size

					puts "#{opposite_type} side is already finished, all #{type} data is scanned, all finished." if verbose
					log.push "#{opposite_type} side is already finished, all #{type} data is scanned, all finished." if remote_debug
					scan_status[type]['finished'] = true
					break
				end
				
				# Scan other side if this side is marked as finished.
				# Scan other side if this orderbook is all checked.
				if scan_status[type]['finished'] || idx >= odbk.size
					scan_buy_now = !scan_buy_now
					next
				end

				o = odbk[idx]
				s, p, p_take = o['s'], o['p'], o['p_take']
				raise "No p_take for #{o} in orderbook" if use_real_price && p_take.nil?
				raise "order data error: #{idx} #{o}\n#{odbk.to_json}" if s.nil? || p.nil? || s==0
				size_sum = scan_status[type]['size_sum']
				avg_price = scan_status[type]['avg_price']
				vol_max_reached = false
				# Scan always stop at this side when max vol is reached.
				if vol_max != nil && s + size_sum > vol_max
					s = vol_max - size_sum
					vol_max_reached = true
					scan_status[type]['finished'] = true
					puts "Max vol reached, vol_max=#{vol_max}" if verbose
					logs.push "Max vol reached, vol_max=#{vol_max}" if remote_debug
				end
				# Cap the size according to maximum order size, again:
				# mkt_client_bid is the market client of bid orderbook, where ask order should be placed.
				# mkt_client_ask is the market client of ask orderbook, where bid order should be placed.
				max_size = nil
				if type == 'buy'
					max_size = mkt_client_ask.max_order_size(pair, type, p)
				elsif type == 'sell'
					max_size = mkt_client_bid.max_order_size(pair, type, p)
				end
				if s + size_sum > max_size
					s = max_size - size_sum
					scan_status[type]['finished'] = true
					balance_limited = true
					puts "Balance limited, #{type} max=#{max_size} p=#{p}" if verbose
					logs.push "Balance limited, #{type} max=#{max_size} p=#{p}" if remote_debug
				end

				# Compare this price with opposite price.
				opposite_p = scan_status[opposite_type]['price']
				opposite_p_take = scan_status[opposite_type]['p_take']
				opposite_s = scan_status[opposite_type]['size_sum']
				# Initializing from this side.
				if opposite_s == 0
					scan_status[type]['idx'] += 1
					scan_status[type]['size_sum'] += s
					scan_status[type]['price'] = p
					scan_status[type]['p_take'] = p_take
					scan_status[type]['avg_price'] = p
					puts "Scan #{type} orderbook #{idx}: #{o} init this side." if verbose
					logs.push "Scan #{type} orderbook #{idx}: #{o} init this side." if remote_debug
					scan_buy_now = !scan_buy_now
					next
				end

				puts "Scan #{type} orderbook #{idx}: #{o}" if verbose
				logs.push "Scan #{type} orderbook #{idx}: #{o}" if remote_debug
				# Compare this price with opposite_p
				price_diff = scan_buy_now ? diff(p, opposite_p) : diff(opposite_p, p)
				if use_real_price
					price_diff = scan_buy_now ? diff(p_take, opposite_p_take) : diff(opposite_p_take, p_take)
				end
				if price_diff <= min_price_diff
					puts "\ttype:#{type} p:#{p},#{p_take} opposite_p:#{opposite_p},#{opposite_p_take} price diff:#{price_diff} < #{min_price_diff} side finished" if verbose
					logs.push "\ttype:#{type} p:#{p},#{p_take} opposite_p:#{opposite_p},#{opposite_p_take} price diff:#{price_diff} < #{min_price_diff} side finished" if remote_debug
					scan_status[type]['finished'] = true
					next
				end
				# Compare future average price, should not deviate from price too much.
				next if size_sum + s == 0
				next_avg_price = (avg_price * size_sum + p * s) / (size_sum + s)
				puts "\tnext_avg:#{next_avg_price.round(8)} diff(next_avg, p):#{diff(next_avg_price, p).abs}" if verbose
				logs.push "\tnext_avg:#{next_avg_price.round(8)} diff(next_avg, p):#{diff(next_avg_price, p).abs}" if remote_debug
				if diff(next_avg_price, p).abs > avg_price_max_diff
					puts "\tdiff(next_avg, p).abs too large, this side finished" if verbose
					logs.push "\tdiff(next_avg, p).abs too large, this side finished" if remote_debug
					scan_status[type]['finished'] = true
					next
				end
				size_sum += s
				# Record scan progress.
				price, avg_price = p, next_avg_price
				scan_status[type]['size_sum'] = size_sum
				scan_status[type]['price'] = price
				scan_status[type]['p_take'] = p_take
				scan_status[type]['avg_price'] = next_avg_price
				puts "\tnow #{type} size_sum:#{size_sum} p:#{price} avg_p:#{next_avg_price}" if verbose
				logs.push "\tnow #{type} size_sum:#{size_sum} p:#{price} avg_p:#{next_avg_price}" if remote_debug
				scan_status[type]['idx'] += 1
				# Compare future size_sum with opposite_s, if this side is fewer, continue scan.
				next if size_sum < opposite_s
				scan_buy_now = !scan_buy_now
			end

			# Return if no suitable orders.
			if scan_status['buy']['idx'] == 0 || scan_status['sell']['idx'] == 0
				return {
					:logs=>[logs, scan_status, [], min_price_diff, opt, odbk_bid[0..8], odbk_ask[0..8]]
				}
			end

			# Scanning finished, generate a pair of order.
			orders = [{'T'=>'buy'}, {'T'=>'sell'}].map do |o|
				o['pair'] = pair unless pair.nil?
				o['s'] = scan_status[o['T']]['size_sum']
				o['p'] = scan_status[o['T']]['price']
				o['p_real'] = scan_status[o['T']]['p_take']
				if o['T'] == 'sell' # Reverse type against to orderbook.
					o['T'] = 'buy'
					o['market'] = market_ask
				else
					o['T'] = 'sell'
					o['market'] = market_bid
				end
				o
			end
			# Make the order corresponded to low capacity market at first.
			orders = orders.sort_by { |o| o['s'] }
			size = orders.map { |o| o['s'] }.min
			# Make pair orders have same size.
			orders = orders.map { |o| o['s'] = size; o }
			# Check if size is smaller than min_order_size
			size_min_limited = orders.any? { |o|
				# mkt_client_bid is the market client of bid orderbook, where ask order should be placed.
				# mkt_client_ask is the market client of ask orderbook, where bid order should be placed.
				client = mkt_client_bid
				client = mkt_client_ask if o['T'] == 'buy'
				o['s'] < client.min_order_size(o)
			}
			orders = nil if size_min_limited

			vol_min_limited = size < vol_min
			orders = nil if vol_min_limited

			puts(JSON.pretty_generate([
				orders,
				balance_limited, vol_min_limited, vol_max_reached, size_min_limited,
				scan_status])) if verbose
			logs.push(JSON.pretty_generate(scan_status)) if remote_debug

			ideal_profit = 0
			if orders != nil
				ideal_profit = (orders[0]['p'] - orders[1]['p']).abs * size
			end
			{
				:orders	=> orders,
				:ideal_profit => ideal_profit,
				:balance_limited	=> balance_limited,
				:vol_min_limited	=> vol_min_limited,
				:vol_max_reached	=> vol_max_reached,
				:size_min_limited => size_min_limited,
				:logs		=> [logs, scan_status, orders, min_price_diff, opt, odbk_bid[0..20], odbk_ask[0..20]]
			}
		end

		# Given orders with different market clients.
		# Format/shrink all orders size according to market rules.
		# Try best to minimize diff of formatted sizes.
		# But this method might break min_vol or min_quantity in some markets.
		# Do size detection after using this.
		def equalize_order_size(orders, clients)
			debug_orders = orders.map { |o| o.clone }
			order_client_list = orders.zip(clients)
			orders_in_vol, orders_in_asset = [], []
			order_client_list.each do |oc|
				case oc[1].quantity_in_orderbook()
				when :vol
					orders_in_vol.push oc
				when :asset
					orders_in_asset.push oc
				else
					raise "Unknown quantity_in_orderbook() #{oc[1].market_name()}"
				end
			end

			if orders_in_vol.empty?
				# If all order sizes are based on asset, choose a minimum one.
				# But this might break min_vol or min_quantity in some markets.
				acceptable_size = order_client_list.map { |oc| oc[1].format_size(oc[0]) }.min
				orders.each { |o| o['s'] = acceptable_size }
				return
			end

			# Some orders are volume based (future contracts), they don't have a precise VOL cap.
			# Compute max_vol from the orders based in asset.
			max_vol_list = orders_in_asset.map { |oc|
				o, c = oc
				# For buy orders, get client available cash as max vol
				# For sell orders, get avaiable balance * price as max vol
				if o['T'] == 'buy'
					order = {'pair'=>o['pair'], 'T'=>o['T'], 'p'=>1.0}
					next c.max_order_size(order)
				elsif o['T'] == 'sell'
					next c.max_order_size(o) * o['p']
				else
					raise "Unknown order type #{o}"
				end
			}
			max_vol = max_vol_list.empty? ? nil : max_vol_list.min

			# orders in volume always come with bigger lot. Shrink vol until not changed.
			# Presume volume should be same between orders
			vol = nil
			loop do
				vol_list = orders_in_vol.map { |oc|
					# For volume based markets, get round(vol) as suggesgt vol.
					o = oc[0]
					t, v, p, s = o['T'], o['v'], o['p'], o['s']
					oc[1].format_vol_str(
						o['pair'], t,
						vol || v || p*s,
						adjust:true, num:true,
						max: max_vol # With a hard cap.
					).to_f
				}
				min_vol = vol_list.min
				if vol.nil? || vol != min_vol
					puts "formated vol_list: #{vol_list} -> #{min_vol} [#{vol}]" if vol != nil
					vol = min_vol
				else
					break
				end
			end
			# Set v and s for orders_in_vol
			# puts "Set v #{vol} for all orders_in_vol"
			orders_in_vol.each do |oc|
				oc[0]['v'] = vol
				oc[0]['s'] = vol/oc[0]['p']
				# puts "Set v #{oc[0]['v']} for:\n#{oc[0]}"
			end
			# Use minimum size in orders_in_vol for orders_in_asset
			min_size = orders_in_vol.map { |oc| oc[0]['s'] }.min
			# puts "Set s #{min_size} for all orders_in_asset"
			orders_in_asset.each do |oc|
				oc[0]['s'] = min_size
				oc[0]['s'] = oc[1].format_size(oc[0])
				# puts "Set s #{oc[0]['s']} for:\n#{oc[0]}"
			end
		end

		# Given markets and orderbooks, buy/sell in best price.
		# max_vol is in btc when type is buy, in asset when type is sell
		# Consider market commision already.
		# TODO Unfinished.
		def market_order(odbk_maps, mkt_clients, type, price_threshold, max_vol)
			odbk_type = (type=='buy' ? 1 : 0)
			mkts = odbk_maps.keys
			scan_index_map = mkts.map { |m| [m, -1] }.to_h
			target_order_map = mkts.map { |m| [m, []] }.to_h
			btc_vol, vol = 0, 0
			loop do
				next_m, next_order = nil, nil
				mkts.each do |m| # Find next order in best price.
					next_idx = scan_index_map[m] + 1
					orders = odbk_maps[mkt][odbk_type]
					next if next_idx >= orders.size
					o = orders[next_idx]
					if next_order.nil?
						next_m, next_order = m, o
					elsif type == 'buy' && o['p_real'] < next_order['p_real']
						next_m, next_order = m, o
					elsif type == 'sell' && o['p_real'] > next_order['p_real']
						next_m, next_order = m, o
					end
				end
				break if next_order.nil? # Searching finished.
				break if type == 'buy' && next_order['p'] > price_threshold
				break if type == 'sell' && next_order['p'] < price_threshold
				btc_vol += next_order['p_real']*next_order['s']
				vol += next_order['s']
				break if type == 'buy' && btc_vol >= max_vol
				break if type == 'sell' && vol >= max_vol
				# Order could be added to final target.
				target_order_map[next_m].push next_order
			end
			target_order_map.each do |m, orders|
			end
		end
	end

	module MarketData
		include APD::ProfilingUtil
		include URN::MarketPairUtil
		include URN::Misc
		include APD::LogicControl

		def redis
			URN::RedisPool
		end

		# Get markets that has valid market data currently.
		def valid_markets(mkts)
			return [] if @_cache_valid_markets.nil?
			# Compute intersection of mkts and @_cache_valid_markets
			return (@_cache_valid_markets & mkts)
		end
		def valid_markets_precompute(mkts)
			@_cache_valid_markets = valid_markets_int(mkts)
		end
		def valid_markets_int(mkts)
			debug ||= @debug
			@_valid_warning ||= {}
			now = (Time.now.to_f * 1000).to_i
			puts ["markets before valid checking:", mkts] if debug
			mkts = mkts.select do |m|
				next false if (@disable_markets || []).include?(m)
				next false if market_client(m).is_banned?()
				begin
					next false if market_client(m).is_banned?()
				rescue URN::NoMarketClient => e
					puts "Initializing additional market client #{m}"
					add_other_market(m)
					next false if market_client(m).is_banned?()
				end
				odbk = @market_snapshot.dig(m, :orderbook)
				next false if odbk.nil? || odbk.empty?
				bids, asks, t = odbk
				next false if bids.nil? || bids.empty?
				next false if asks.nil? || asks.empty?
				gap = now - t.to_i
				abort_reason = "#{gap/1000} seconds ago" if gap > 3600*1000
				if abort_reason != nil
					@_valid_warning[m] ||= 0
					if now - @_valid_warning[m] > 10*1000
						puts "#{m} #{@pair} orderbook aborted, #{abort_reason}".red
						@_valid_warning[m] = now
					end
					next false
				end
				next false if [bids, asks].include?(nil)
				next false if [bids, asks].include?([])
				true
			end
			mkts = mkts - (@disable_markets || []) # Maybe some markets are disabled.
			puts ["Valid markets:", mkts] if debug
			mkts
		end

		# Support pair_list as single pair or list.
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		# if opt[:no_real_p] is true, stop computing real price for better speed.
		#
		# Normally used when broadcast with full snapshot:
		# if opt[:data] is given, data would be used directly but not from latest_orderbook()
		#
		# Defensive code is removed because high frequency calling.
		def refresh_orderbooks(mkt_clients, pair_list, snapshot, opt={})
			if pair_list.is_a?(String)
				pair_list = [pair_list]*mkt_clients.size()
			elsif pair_list.is_a?(Array)
				;
				# if pair_list.size() != mkt_clients.size()
				#		raise "Unconsistent pair_list #{pair_list.size()} mkts #{mkt_clients.size()}"
				# end
			else
				raise "Unknown pair_list type #{pair_list.class}"
			end

			@_valid_warning ||= {}
			data_chg = false
			now = (Time.now.to_f*1000).to_i
			odbk_list = opt[:data] || latest_orderbook(mkt_clients, pair_list, opt)
			mkt_clients.zip(odbk_list, pair_list).each do |client, odbk, pair|
				next if odbk.nil?
				# Add real price with commission to each order
				_preprocess_orderbook(pair, odbk, client) if opt[:no_real_p] != true
				snapshot[client.given_name] ||= {}
				snapshot[client.given_name][:orderbook] = odbk
				bids, asks, t, mkt_t = odbk
				# Abort if timestamp is too old compared to system timestamp.
				abort_reason = nil
				gap = now - t.to_i
				abort_reason = "#{gap/1000} seconds ago" if gap > 60*1000
				if abort_reason != nil
					m = client.market_name()
					@_valid_warning[m] ||= 0
					if now - @_valid_warning[m] > 600*1000
						puts "#{m} #{pair} odbk -X-> data_chg, #{abort_reason}".red
						@_valid_warning[m] = now
					end
					next
				end
				# Check market timestamp with latest market_client timestamp.
				time_legacy = client.last_operation_time.strftime('%Q').to_i - t
				if time_legacy >= 0
					m = client.market_name()
					puts "#{m} orderbook is #{time_legacy}ms old"
					next
				end
				data_chg = true
			end
			data_chg
		end

		# Get latest orderbook from market/pair
		# Support pair_list as single pair or list.
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		# Defensive code is removed because high frequency calling.
		def latest_orderbook(mkt_clients, pair_list, opt={})
			if pair_list.is_a?(String)
				pair_list = [pair_list]*mkt_clients.size()
			elsif pair_list.is_a?(Array)
				;
				# if pair_list.size() != mkt_clients.size()
				#		raise "Unconsistent pair_list #{pair_list.size()} mkts #{mkt_clients.size()}"
				# end
			else
				raise "Unknown pair_list type #{pair_list.class}"
			end
			# Optional: Use order_pairs in generating orderbook.
			if opt[:order_pairs] != nil
				# if opt[:order_pairs].size() != mkt_clients.size()
				#		raise "Unconsistent order_pairs#{opt[:order_pairs].size()} mkts #{mkt_clients.size()}"
				# end
				puts(["Pair:", opt[:order_pairs]]) if opt[:debug]
			end

			keys = opt[:redis_channles] || mkt_clients.zip(pair_list).map do |mkt_client, pair|
				market = mkt_client.market_name()
				odbk_m = market.split('_').first
				"URANUS:#{odbk_m}:#{local_pair(pair, mkt_client)}:orderbook"
			end
			puts(["Redis:", keys]) if opt[:debug]
			profile_record_start(:read_market_data)
			msgs = endless_retry(sleep:0.2) { redis.mget(*keys) }
			puts(["Redis data:", msgs.map { |m| m.nil? ? 'NIL' : m.size }]) if opt[:debug]
			profile_record_end(:read_market_data)
			@_last_odbk_cache ||= {}
			cache = @_last_odbk_cache
			# Use provided order_pairs in processing orderbook and order.
			odbk_list = mkt_clients.zip(msgs, (opt[:order_pairs] || pair_list)).map do |mkt_client, msg, pair|
				market = mkt_client.market_name()
				next nil if cache[mkt_client.given_name] == msg
				cache[mkt_client.given_name] = msg
				bids, asks, t, mkt_t = parse_json(msg)
	
				# Convert data, parse string into float
				bids, asks = [bids, asks].map do |orders|
					orders.map do |o|
						o['market'] = market
						o['pair'] = pair
						o['s'] = o['s'].to_f
						o['p'] = o['p'].to_f
						o
					end
				end
	
				# Clean error data for orderbook.
				asks = asks.select { |o| o['s'] > 0 } if asks.size > 0
				bids = bids.select { |o| o['s'] > 0 } if bids.size > 0
				new_asks = asks.select { |o| o['p'] > bids.first['p'] } if bids.size > 0
				new_bids = bids.select { |o| o['p'] < asks.first['p'] } if asks.size > 0
				asks, bids = new_asks, new_bids

				# Ignore if orderbook data seems not ready.
				if asks.nil? || asks.size < 5 || bids.nil? || bids.size < 5
					puts "orderbook data seems not ready #{market} #{pair}"
					next nil
				end

				if opt[:max_depth] != nil # Filter top depth odbk.
					max_depth = opt[:max_depth]
					bids = bids[0..max_depth]
					asks = asks[0..max_depth]
				end
	
				odbk = [bids, asks, t, mkt_t]
				next odbk
			end
			odbk_list
		end

		# Add p_real to orderbook, indicates real price with commission.
		def _preprocess_orderbook(pair, orderbook, mkt_client)
			bids, asks, t = orderbook
			bids ||= []
			asks ||= []
			# Speed up. round() -> 26.52%, (-) -> 9.60%
			# precise = @price_precise || 10
# 			bids.each { |o| o['p_take'] ||= (o['p']*(1-rate)).round(precise) }
# 			asks.each { |o| o['p_take'] ||= (o['p']/(1-rate)).round(precise) }
# 			bids.each { |o| o['p_make'] ||= (o['p']/(1-rate)).round(precise) }
# 			asks.each { |o| o['p_make'] ||= (o['p']*(1-rate)).round(precise) }

			r = 1 - mkt_client.preprocess_deviation(pair, t:'taker/sell')
			bids.each { |o| o['p_take'] ||= o['p']*r }
			r = 1 - mkt_client.preprocess_deviation(pair, t:'taker/buy')
			asks.each { |o| o['p_take'] ||= o['p']/r }
			r = 1 - mkt_client.preprocess_deviation(pair, t:'maker/buy')
			bids.each { |o| o['p_make'] ||= o['p']/r }
			r = 1 - mkt_client.preprocess_deviation(pair, t:'maker/sell')
			asks.each { |o| o['p_make'] ||= o['p']*r }
			orderbook
		end

		# Support pair_list as single pair or list.
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		def refresh_trades(mkt_clients, pair_list, snapshot, opt={})
			if pair_list.is_a?(String)
				pair_list = [pair_list]*mkt_clients.size()
			elsif pair_list.is_a?(Array)
				if pair_list.size() != mkt_clients.size()
					raise "Unconsistent pair_list #{pair_list.size()} mkts #{mkt_clients.size()}"
				end
			else
				raise "Unknown pair_list type #{pair_list.class}"
			end

			data_chg = false
			trade_his_list = latest_trades mkt_clients, pair_list, opt
			mkt_clients.zip(trade_his_list, pair_list).each do |client, trade_his, pair|
				next if trade_his.nil?
				market = client.market_name()
				snapshot[market][:trades] = trade_his
				trades, t = trade_his
				# Abort if timestamp is too old compared to system timestamp.
				abort_reason = nil
				gap = (Time.now.to_f*1000).to_i - t
				abort_reason = "timestamp is #{gap/1000} seconds ago" if gap > 600*1000
				if abort_reason != nil
					puts "#{market} trade history aborted, #{abort_reason}".red
					next
				end
				# Check market timestamp with latest market_client timestamp.
				time_legacy = client.last_operation_time.strftime('%Q').to_i - t
				if time_legacy >= 0
					puts "#{market} trade history is #{time_legacy}ms old"
					next
				end
				precise = @price_precise || 10
				data_chg = true
			end
			data_chg
		end

		# Support pair_list as single pair or list.
		# if opt[:order_pairs] is given as list, use this in order['pair'] and p_real()
		def latest_trades(mkt_clients, pair_list, opt={})
			if pair_list.is_a?(String)
				pair_list = [pair_list]*mkt_clients.size()
			elsif pair_list.is_a?(Array)
				if pair_list.size() != mkt_clients.size()
					raise "Unconsistent pair_list #{pair_list.size()} mkts #{mkt_clients.size()}"
				end
			else
				raise "Unknown pair_list type #{pair_list.class}"
			end
			# Optional: Use order_pairs in generating orderbook.
			if opt[:order_pairs] != nil
				if opt[:order_pairs].size() != mkt_clients.size()
					raise "Unconsistent order_pairs#{opt[:order_pairs].size()} mkts #{mkt_clients.size()}"
				end
				puts(["Pair:", opt[:order_pairs]]) if opt[:debug]
			end

			keys = mkt_clients.zip(pair_list).map do |mkt_client, pair|
				market = mkt_client.market_name()
				odbk_m = market.split('_').first
				"URANUS:#{odbk_m}:#{local_pair(pair, mkt_client)}:trades"
			end
			puts(["Redis:", keys]) if opt[:debug]
			profile_record_start(:read_trades_data)
			msgs = endless_retry(sleep:0.2) { redis.mget(*keys) }
			profile_record_end(:read_trades_data)
			@_last_tick_cache ||= {}
			cache = @_last_tick_cache
			# Use provided order_pairs in processing trades.
			trades_list = mkt_clients.zip(msgs, (opt[:order_pairs] || pair_list)).map do |mkt_client, msg, pair|
				market = mkt_client.market_name()
				next nil if cache[market] == msg
				cache[market] = msg
				trades, t = parse_json(msg)
				trades ||= []
				trades = parse_new_market_trades(market, pair, trades)
				next [trades, t]
			end
			trades_list
		end

		# Parse new market trades from broadcast data directly.
		def parse_new_market_trades(market, pair, data)
			return data.map { |trade|
				trade['pair'] = pair
				trade['market'] = market
				if trade['remained'] != 0 # new spider would do this already.
					trade['T'] = trade['T'].downcase
					trade['s'] = trade['s'].to_f
					trade['p'] = trade['p'].to_f
					trade['executed'] = trade['s'].to_f
					trade['remained'] = 0
					trade['status'] = 'filled'
					trade['t'] = DateTime.parse("#{trade['t']}+0800}").strftime('%Q')
				end
				trade
			}
		end
	end
	
	# For switching market data implementions.
	# With this agent, trader class does not need to include MarketData directly.
	# So it would be replaced by broadcast data source, or others.
	class DirectQueryMarketDataAgent
		include MarketData
		def initialize(mgr, market_snapshot, disable_markets=[])
			@client_mgr = mgr
			@market_snapshot = market_snapshot
			@disable_markets = disable_markets
			@_last_odbk_cache = {}
			@_last_tick_cache = {}
		end

		def market_client(mkt)
			@client_mgr.market_client(mkt)
		end

		def all_market_data_ready?(markets)
			missed_mkts = valid_markets(markets) - @_last_odbk_cache.keys()
			return true if missed_mkts.empty?
			puts "Market data #{missed_mkts} is not ready."
			return false
		end
	end
end

######### TEST #########
if __FILE__ == $0 && defined? URN::BOOTSTRAP_LOAD
	include URN::OrderUtil
	include URN::TradeUtil
	include URN::AssetManager
	if ARGV[0] == 'test1'
		puts "Market client testing aggressive_orders ..."
		test_file = "./test_input/aggressive_orders/#{ARGV[1]}.json"
		abort "Input #{test_file} is not found" unless File.file? test_file
		json = JSON.parse(File.read(test_file))
		price_threshold, type, opt, orderbook = json[2..5]
		opt = opt.to_a.map { |kv| [kv[0].to_sym, kv[1]] }.to_h
		opt[:verbose] = true
		opt[:remote_debug] = false
		puts opt
		r = nil
		if json[1]['pair'].nil? || json[1]['market'].nil?
			r = aggressive_order(orderbook, price_threshold, type, opt)
		else
			desired_o = json[1]
			mkt_client = URN.const_get(desired_o['market']).new skip_balance:true
			r = aggressive_order(orderbook, desired_o, mkt_client, opt)
		end
 		r.delete :logs
		puts JSON.pretty_generate(r)
		exit
	elsif ARGV[0] == 'test2'
		URN::TRADE_MARKETS_DUPACCOUNT.each do |m|
			client_register URN.const_get(m).new(verbose:true, skip_balance:true, trade_mode:'test')
		end
		puts "Market client testing aggressive_arbitrage_orders ..."
		test_file = "./test_input/aggressive_arbitrage_orders/#{ARGV[1]}.json"
		abort "Input #{test_file} is not found" unless File.file? test_file
		json = JSON.parse(File.read(test_file))
		min_price_diff, opt, odbk_bid, odbk_ask = json[3..6]
		opt = opt.to_a.map { |kv| [kv[0].to_sym, kv[1]] }.to_h
		opt[:verbose] = true
		pair = 'BTC-BAT'
		_preprocess_orderbook(pair, [odbk_bid, [], nil], URN.const_get(opt[:market_bid]).new(verbose:true, skip_balance:true))
		_preprocess_orderbook(pair, [[], odbk_ask, nil], URN.const_get(opt[:market_ask]).new(verbose:true, skip_balance:true))
		opt[:use_real_price] = true
		puts opt
		r = aggressive_arbitrage_orders odbk_bid, odbk_ask, min_price_diff, opt
		r.delete :logs
		puts JSON.pretty_generate(r)
		exit
	elsif ARGV[0] == 'test3'
		hitbtc = URN::HitBTC.new
		puts hitbtc.format_price_str('BTC-DNT', 'buy', 0.0000119, adjust:true)
		abort "Test3-1 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.0000119, adjust:true, num:true) != 0.000011
		abort "Test3-1 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.0000119, adjust:true) != '0.000011'
		abort "Test3-2 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.0000119, adjust:true, num:true) != 0.000012
		abort "Test3-2 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.0000119, adjust:true) != '0.000012'
		abort "Test3-3 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.000011) != '0.000011'
		abort "Test3-4 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.000011) != '0.000011'
		abort "Test3-5 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 11) != '11'
		abort "Test3-5 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 11) != '11'
		puts "test3 passed"
		exit
	elsif ARGV[0] == 'test4'
		['Bittrex', 'Huobi'].each do |m|
			client_register URN.const_get(m).new(verbose:true, skip_balance:true, trade_mode:'test')
		end
		['Bittrex', 'Huobi'].each do |m|
			order = {'market'=>m, 'p'=>0.00002602, 's'=>1, 'pair'=>'BTC-BAT','T'=>'sell'}
			price_real_set(order, 0.00002602)
			rate = market_client(m).fee_rate_real(order['pair'], nil,"taker/#{order['T']}")
			puts "rate:#{rate}"
			puts "mkt:#{m}, p:#{order['p']}, p_real:#{order['p_real']}"
			cp = price_real(order, m)
			puts "mkt:#{m}, p:#{order['p']}, p_real:#{order['p_real']}"
		end
		exit
	elsif ARGV[0] == 'compare_bal'
		URN::TRADE_MARKETS_DUPACCOUNT.each do |m|
			client_register URN.const_get(m).new(verbose:true, skip_balance:true, trade_mode:'test')
		end
	
		bal = balance_all
	
		# Print balance with type.
		print "#{'Total'.ljust(5)} #{format_num('CASH', 6)}      % #{format_num('RESERVED', 6)}\n".blue
		bal[:bal].each do |asset, v|
			base = ENV["URANUS_BASE_#{asset}"]
			next if base.nil?
			ttl = base.to_f
			cash_ttl = v['cash']
			p = "#{(cash_ttl/ttl * 100).round.to_s.rjust(3)}%"
			print "#{asset.ljust(8)}#{format_num(v['cash'], 6)} #{p} #{format_num(v['reserved'], 6)}\n".blue
			bal[:bal_map].each do |market, mkt_bal|
				mkt_v = mkt_bal[asset]
				next if mkt_v.nil?
				max_ttl = [ttl, cash_ttl].max
				cash_percent = (mkt_v['cash']/max_ttl * 100).round
				cp = "#{cash_percent.to_s.rjust(3)}%"
				# Special case for bittrex, it uses less BTC.
				if asset == 'BTC'
					if market == 'Bittrex'
						cp = cp.light_red if mkt_v['cash'] < 2.5
						cp = cp.light_white.on_light_red if mkt_v['cash'] < 1.5
					else
						cp = cp.light_red if mkt_v['cash'] < 4
						cp = cp.light_white.on_light_red if mkt_v['cash'] < 2
					end
				else
					cp = cp.light_red if cash_percent < 25
					cp = cp.light_white.on_light_red if cash_percent < 10
				end
				print "#{market.ljust(8)}#{format_num(mkt_v['cash'], 6)} #{cp} #{format_num(mkt_v['reserved'], 6)}\n"
			end
			print "\n"
		end
	
		# Compare to base.
		bal[:ttl].each do |asset, res|
			base = ENV["URANUS_BASE_#{asset}"]
			next if base.nil?
			base = base.to_f
			diff = (res - base)
			if diff > 0
				puts "#{asset.ljust(8)}#{diff.round(5)}".green
			else
				puts "#{asset.ljust(8)} #{diff.round(5)}".red
			end
		end
	elsif ARGV[0] == 'testall'
		URN::OMSLocalCache.monitor(URN::TRADE_MARKETS, [Thread.current])
		# Run all test for all trade market.
		URN::TRADE_MARKETS.each do |m|
			client = client_register URN.const_get(m).new(verbose:true, trade_mode:'test')
			puts "Running test_trading_process() for #{m}".blue
			client.test_trading_process()
		end
		URN::TRADE_MARKETS.each do |m|
			puts "√ test_trading_process() passed #{m}".green
		end
	elsif ARGV[0] == 'proxy_benchmark'
		URN::OMSLocalCache.monitor(URN::TRADE_MARKETS, [Thread.current], verbose:false)
		ports = ARGV[1..-1]
		raise("Need socks port") if ports.empty?
		results = {}
		ttl_round = 5
		ports.each { |port|
			results[port] = {}
			if port == 'default'
				proxy_str = 'default'
			else
				proxy_str = "socks://127.0.0.1:#{port}"
			end
			# Run all test N times for all trade market.
			clients = URN::TRADE_MARKETS.map do |m|
				next if m == 'Bitmex' # Bitmex restrict US/HK IP
				next if m == 'Bitstamp' # Bitstamp restrict API to single IP
				next if m == 'Kraken' # Kraken limit API freq too much.
				if m == 'HBDM'
					ENV["HUOBI_API_PROXY"] = proxy_str
				else
					ENV["#{m.upcase}_API_PROXY"] = proxy_str
				end
				clazz = URN.const_get(m)
				# Overwrite oms_enabled?()
				clazz.define_method(:'oms_enabled?') { false }
				client = client_register(clazz.new(verbose:false, skip_balance:true, trade_mode:'test'))
				client
			end.select { |c| c != nil }
			# Warmup
			clients.each { |c|
				c.balance()
				loop {
					break if URN::OMSLocalCache.support_mkt?(c.market_name())
					puts "Wait for #{c.market_name()} OMS started"
					sleep 1
				}
			}
			clients.each { |c|
				ttl_round.times.each { |n|
					start_t = Time.now.to_f
					c.test_trading_process()
					test_t = (Time.now.to_f - start_t).to_f.round(2)
					results[port][c.market_name()] ||= []
					results[port][c.market_name()].push(test_t)

					# Print benchmark board after each test
					ports.each { |p|
						puts "PORT #{p}".blue
						clients.each { |c|
							row = [c.market_name().ljust(8)] + 
								(results.dig(p, c.market_name()) || []).map { |s| s.to_s.rjust(6) }
							print "#{row.join}\n".blue
						}
					}
				}
			}
		}
	else
		puts "Unknown args [#{ARGV}]"
	end
end
