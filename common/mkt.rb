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
			raise "order have invalid i #{o}" if o['i'] == '0' # Defensive checking for potential bugs
			puts "order_remember:\n#{format_trade(o)}" if opt[:verbose] == true
			@managed_orders ||= {}
			@managed_orders[o['pair']] ||= {}
			old_order = @managed_orders.dig(o['pair'], o['i'])
			if old_order != nil
				unless order_same?(old_order, o) # Defensive check
					raise "old_order and o aren't same\n#{old_order}\n#{o}"
				end
				puts "order_replace:\n#{format_trade(o)}" if opt[:verbose] == true
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
			@managed_orders.dig(o['pair'], o['i']) != nil
		end

		def order_managed(pair, i)
			@managed_orders ||= {}
			@managed_orders.dig(pair, i)
		end
		thread_safe :order_remember, :order_forget, :order_forget_all, :order_managed?

		def _balance_avail(pair, type, price, opt={})
			balance() if @balance_cache.nil?
			raise "Price should be specfied" if price.nil?
			pair = pair_to_underlying_pair(pair)
			asset1, asset2 = pair_assets(pair)

      if @_is_ib == true
				remain = 0
        if market_type() == :spot
          if ['buy', 'bid'].include?(type.strip.downcase)
            cash = (@balance_cache.dig(asset1, 'cash') || 0) - remain
            return cash/price
          elsif ['sell', 'ask'].include?(type.strip.downcase)
            # Look for asset in name of ib_exchange:STK:pair
            ib_exchange = self.class.name.split('::').last
            ib_pair = "#{ib_exchange}:STK:#{pair}"
            if ib_exchange == 'SMART' # Decide asset real market for SMART
              ib_pair = @balance_cache.keys.select { |p| p.end_with?(":STK:#{pair}") }
              if ib_pair.size == 1
                ib_pair = ib_pair[0] # Select this by default.
              elsif ib_pair.size == 0
                return 0
              else
                raise "Multi asset matches: #{ib_pair}"
              end
            end
            return (@balance_cache.dig(ib_pair, 'cash') || 0) - remain
          end
          raise "unknown type #{type}"
        else
          raise "Not implemented"
        end
			elsif market_type() == :spot
				# For spot market, keep balance 0.0001 by default
				remain = 0.0001 # Default: always keep some balance.
				left_ratio = 0.0 # Default: use up all balance.
				if opt[:clear_bal] == true
					remain = 0
					if type == 'buy' && market_name() == 'HitBTC' && pair =~ /^USDT/
						# HitBTC needs to keep some USDT for fee, and it is 0.1%, not maker/buy fee
						left_ratio = 0.001
					end
				end
				if ['buy', 'bid'].include?(type.strip.downcase) && market_name() == 'FTX' && asset1 == 'USD'
					remain = 200_000 # To buy with USD, keep at least 200K USD in FTX for futures margin.
				elsif ['buy', 'bid'].include?(type.strip.downcase) && market_name() == 'FTX' && asset1 == 'BTC'
					remain = 30 # To buy with BTC, keep at least 30 BTC in FTX for futures margin.
				elsif ['sell', 'ask'].include?(type.strip.downcase) && market_name() == 'FTX' && asset2 == 'BTC'
					remain = 30 # To sell XXX-BTC, keep at least 30 BTC in FTX for futures margin.
				end

				if ['buy', 'bid'].include?(type.strip.downcase)
					cash = (@balance_cache.dig(asset1, 'cash') || 0)*(1-left_ratio) - remain
					return cash/price
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					return (@balance_cache.dig(asset2, 'cash') || 0)*(1-left_ratio) - remain
				end
				raise "unknown type #{type}"
			elsif market_type() == :future
				; # Process this below.
			else
				raise "unknown market type #{market_type()}"
			end

			########### Process for future markets ############
			########### How much safe margin left ############
			cash_asset = future_margin_asset(pair)
			cash_in_asset1 = (cash_asset == asset1)
			cash_in_asset1 = true if market_name == 'FTX' # FTX use FTX_COLLATERAL, is also kind of USD

			# Don't reduce cash by remain for futures.
			# Position cost of each pair should not larger than future_max_position_cost()
			cash = future_available_cash(pair, type)
			position = future_position(pair)
			if position == 0
				if ['buy', 'bid'].include?(type.strip.downcase)
					lev = future_max_long_leverage(pair)
					if cash_in_asset1 # Use A to long A-B
						cash *= lev
						return cash/price
					end
					return cash*(lev-1) # Use B to long A-B
				elsif ['sell', 'ask'].include?(type.strip.downcase)
					lev = future_max_short_leverage(pair)
					if cash_in_asset1 # Use A to short A-B
						cash *= lev
						return cash/price
					end
					return cash*lev # Use B to short A-B
				else
					raise "unknown type #{type}"
				end
			elsif position > 0 # Long position
				if ['buy', 'bid'].include?(type.strip.downcase)
					lev = future_max_long_leverage(pair)
					if cash_in_asset1
						cash *= lev
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
						cash *= lev
						return cash/price
					end
					return cash*lev
				end
				raise "unknown type #{type}"
			end
		end

		# For markets with USD/USDT cross margin:
		# 	If 1 contracts holding % more than 30%, max_lev *= 1
		# 	If less than 5 contracts holding, max_lev *= 1
		#
		# 	If more than 5 contracts holding, max pos% < 15%, max_lev *= 2 when pos < 15%
		# 	If more than 5 contracts holding, max pos% < 30%, max_lev *= 1.5
		def future_diversity_mul(pair=nil)
			return 1 if quantity_in_orderbook() == :vol
			# FTX does not need more mul, need to take care of USD balance
			return 1 if market_name() == 'FTX'

			larger_5_pct_contracts = []
			larger_10_pct_contracts = []
			larger_30_pct_contracts = []
			pct_ratio_map = {}
			pos_ttl = 0
			@balance_cache.each { |contract, data|
				next unless is_future?(contract)
				pos_v = (data['cash_v'] || 0) + (data['reserved_v'] || 0)
				pos_ttl += pos_v.abs
			}
			return 1 if pos_ttl == 0
			@balance_cache.each { |contract, data|
				next unless is_future?(contract)
				pos_v = (data['cash_v'] || 0) + (data['reserved_v'] || 0)
				pct_ratio_map[contract] = ratio = (pos_v.abs / pos_ttl.to_f * 100).round
				larger_30_pct_contracts.push(contract) if ratio > 30
				larger_10_pct_contracts.push(contract) if ratio > 10
				larger_5_pct_contracts.push(contract) if ratio > 5
			}

			pct_list = pct_ratio_map.values.sort.reverse

			return 1 if pct_list.empty?
			return 1 if pct_list[0] > 30
			return 1 if pct_list.size < 5

			# More than 5 contracts below, if pair is specified and pair_pos is low, mul = 2
			if pct_list[0] <= 15
				return 2 if pair != nil && (pct_ratio_map[pair].nil? || pct_ratio_map[pair] < 15)
			end
			return 1.5
		end

		# Max long position could be open with 1 asset
		def future_max_long_leverage(pair=nil)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)

			max_lev = nil
			if market_name() == 'FTX'
				# FTX use FTX_COLLATERAL as margin but need to compute USD PnL buffer
				usd_bal = @balance_cache.dig('USD', 'cash')
				usd_bal += (@balance_cache.dig('USD', 'reserved') || 0)
				usd_bal += 30000 # FTX allows 30000 debt at most.
				collateral = @balance_cache.dig('FTX_COLLATERAL', 'cash')
				collateral += (@balance_cache.dig('FTX_COLLATERAL', 'reserved') || 0)
				# Use [USD bal * 10, collateral].min as collateral
				if 10*usd_bal < collateral
					max_lev = (1.5 * (10*usd_bal / collateral.to_f)).ceil(2)
				else
					max_lev = 1.5
				end
			else
				max_lev = 1.5
			end

			return max_lev * future_diversity_mul(pair)
		end

		# Max short position could be open with 1 asset
		def future_max_short_leverage(pair=nil)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)

			max_lev = nil
			if market_name() == 'FTX'
				# FTX use FTX_COLLATERAL as margin but need to compute USD PnL buffer
				usd_bal = @balance_cache.dig('USD', 'cash')
				usd_bal += (@balance_cache.dig('USD', 'reserved') || 0)
				usd_bal += 30000 # FTX allows 30000 debt at most.
				collateral = @balance_cache.dig('FTX_COLLATERAL', 'cash')
				collateral += (@balance_cache.dig('FTX_COLLATERAL', 'reserved') || 0)
				# Use [USD bal * 10, collateral].min as collateral
				if 10*usd_bal < collateral
					max_lev = (1.5 * (10*usd_bal / collateral.to_f)).ceil(2)
				else
					max_lev = 1.5
				end
			elsif quantity_in_orderbook() == :vol
				max_lev = 2 # Higher leverage for coin-margin market
			else
				max_lev = 1.5
			end

			return max_lev * future_diversity_mul(pair)
		end

		def future_position(pair)
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			asset1, asset2 = pair_assets(pair) # asset2 is contract code
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
			asset1, contract = pair_assets(pair)
			if quantity_in_orderbook() == :asset
				cost = nil
				loop {
					position = future_position(pair)
					cost = (@balance_cache.dig(contract, 'cost') || 0)
					if position != 0 && cost == 0
						puts "#{market_name()} #{pair} pos #{position} but cost in balance cache is zero".red
						puts "Is this position just opened? Wait 1 second to refresh balance".red
						keep_sleep 1
						balance(allow_fail: true)
					else
						break
					end
				}
				return cost
			elsif quantity_in_orderbook() == :vol
				return (@balance_cache.dig(contract, 'cash') || 0) + (@balance_cache.dig(contract, 'reserved') || 0)
			else
				raise "Unknown quantity_in_orderbook()"
			end
		end

		# Max position cost in BTC for given pair.
		# TODO Better integrate into current preprocess_deviation()
		def future_max_position_cost(pair, type)
			raise "Only for future market." unless market_type() == :future
			raise "Override this method"
		end

		# See future_margin_asset()
# 		def future_collateral_asset(pair)
# 			raise "For futures only #{pair}" unless is_future?(pair)
# 			quote, asset, expiry = parse_contract(pair)
# 			if market_name() == 'Bitmex' && pair.start_with?('BTC-')
# 				return quote
# 			elsif market_name() == 'FTX'
# 				return 'FTX_COLLATERAL'
# 			elsif ['Bybit'].include?(market_name()) && pair.start_with?('USDT-')
# 				return quote
# 			elsif ['BNUM', 'BybitU', 'BybitU_LHY'].include?(market_name()) && (pair.start_with?('USDT-') || pair.start_with?('BUSD-'))
# 				return 'USDT'
# 			elsif ['HBDM', 'Bybit', 'Bybit_LHY', 'BNCM', 'BNCM_Y'].include?(market_name()) && pair.start_with?('USD-')
# 				return asset
# 			else
# 				raise "Not implemented for #{pair} in #{market_name()}"
# 			end
# 		end

		def future_margin_asset(pair)
			raise "For futures only #{pair}" unless is_future?(pair)
			if market_name() == 'FTX'
				return "FTX_COLLATERAL"
			elsif quantity_in_orderbook() == :vol # USD-BTC@P -> BTC
				return pair.split('@')[0].split('-')[1]
			else
				# BUSD-BTC@P -> BUSD
				# USDT-BTC@P -> USDT
				return pair.split('-')[0]
			end
		end

		# Compute available cash asset for open new position
		# For Bitmex(BTC based) (which uses BTC for *every* contract margin):
		# For Bybit USDT contract (which uses USDT for *every* USDT contract margin):
		# For BNCM/HBDM/Bybit (USD) (which uses asset2(USD-asset2) for *related* contract margin):
		# 	Available cash = total_asset - sum(pending_buying_orders)/MAX_LEVERAGE - sum(position_cost)/MAX_LEVERAGE
		# 	Position cost of each pair should not be larger than future_max_position_cost()
		def future_available_cash(pair, type, opt={})
			raise "Only for future market." unless market_type() == :future
			pair = get_active_pair(pair)
			cash_asset = nil
			asset1, asset2, expiry = parse_contract(pair)
			cash_asset = future_margin_asset(pair)
			cost_on_quote = (cash_asset == asset1) # quote-base contract use quote as margin asset.
			cost_on_quote = true if market_name == 'FTX' # FTX use FTX_COLLATERAL, is also kind of USD

			# Available balance should deduct from pending buying orders.
			cash_balance = (@balance_cache.dig(cash_asset, 'cash') || 0)
			cash_balance -= (@balance_cache.dig(cash_asset, 'reserved') || 0)/future_max_long_leverage()
			# Total cost of pair, total cost of long and short
			pair_cost, long_cost, short_cost = 0.0, 0.0, 0.0
			@balance_cache.each { |contract, bal_map|
				next unless is_future?(contract)
				if market_name() == 'FTX'
					# All FTX contracts uses FTX_COLLATERAL
				elsif cost_on_quote == true
					# example: Bybit all USDT contract shares USDT as margin.
					next unless contract.start_with?("#{cash_asset}-")
				elsif cost_on_quote == false
					# example: HBDM/Bybit uses ETH for USD-ETH contracts only
					# USD-ETH@20190927 matches (-ETH@)
					# USD-ETH@20190927 and USD-ETH@20190913 share same margin.
					next unless contract.include?("-#{cash_asset}@")
				end
				cash, reserved, cost = ['cash', 'reserved', 'cost'].map do |k|
					if k == 'cost' && bal_map[k].nil?
						bal_map[k] = 0 # New opened position might have zero cost at first.
					end
					raise "No #{contract} #{k} in #{@balance_cache}" if bal_map[k].nil?
					bal_map[k]
				end
				# Asset reserve always means pending selling orders.
				# Total long position = cash + reserve
				# Total short position = cash + reserve
				position = cash + reserved
				# Defensive check
				# Would cause crash, cost is not self-maintained along with balance, need refresh result from API
				# if (position == 0 && cost != 0) || (position != 0 && cost == 0)
				# 	raise "Position and cost unconinstent in #{contract} of #{@balance_cache}"
				# end
				value = 0
				if position > 0 # Long position
					lev = future_max_long_leverage(contract)
					value = cost_on_quote ? (cost.abs() / lev) : (position.abs() / (lev-1))
					long_cost += value
					pair_cost += value if type == 'buy'
				elsif position < 0 # Short position
					lev = future_max_short_leverage(contract)
					value = cost_on_quote ? (cost.abs() / lev) : (position.abs() / lev)
					short_cost += value
					pair_cost += value if type == 'sell'
				end
				if opt[:verbose] == true
					puts [
						"Pair #{contract}",
						"cost #{cost} #{asset1}",
						"position #{position} #{asset2}",
						"Max lev #{lev}",
						"cost w/ lev #{value} #{cost_on_quote ? asset1 : asset2}"
					].join(', ')
				end
			}
			available_cash = (cash_balance - long_cost - short_cost).floor(8)
			max_cost = future_max_position_cost(pair, type)
			allocate_cash = [available_cash, max_cost-pair_cost].min
			if opt[:verbose] == true
				puts "ALL cost L #{long_cost} S #{short_cost} Pair #{pair} #{type} cost: #{pair_cost}"
				puts "Max cross leverage: L #{future_max_long_leverage()} S #{future_max_short_leverage()}"
				puts "Max #{pair} leverage: L #{future_max_long_leverage(pair)} S #{future_max_short_leverage(pair)}"
				puts "#{cash_asset} balance: #{cash_balance} Avail: #{available_cash} Max cost: #{max_cost} Allowed: #{allocate_cash}"
			end
			allocate_cash
		end

		def balance_cache
			@balance_cache
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

		def balance_ttl_cash_asset(pair)
			pair = pair_to_underlying_pair(pair)
			currency, asset = pair_assets(pair)
			bal_cash = @balance_cache.dig(currency, 'cash')||0
			bal_cash += @balance_cache.dig(currency, 'reserved')||0
			bal_asset = @balance_cache.dig(asset, 'cash')||0
			bal_asset += @balance_cache.dig(asset, 'reserved')||0
			[bal_cash, bal_asset]
		end

		def balance_free_cash_asset(pair)
			pair = pair_to_underlying_pair(pair)
			currency, asset = pair_assets(pair)
			bal_cash = @balance_cache.dig(currency, 'cash')||0
			bal_asset = @balance_cache.dig(asset, 'cash')||0
			[bal_cash, bal_asset]
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
			old_price = price
			price = format_price_str(pair, type, price, adjust:true, num:true)
			if price <= 0 # Redo verbosely then raise error.
				format_price_str(pair, type, old_price, adjust:true, num:true, verbose: true)
				raise URN::OrderArgumentError.new("Invalid formatted price #{old_price} -> #{price}")
			end
			balance = _balance_avail(pair, type, price, opt)
			if ['buy', 'bid'].include?(type.strip.downcase) # price is passed into _balance_avail() already
				# Not sure maker/taker
				# return (balance/price)*(1.0-rate))
			elsif ['sell', 'ask'].include?(type.strip.downcase)
				;
			else
				raise "max_order_size: unknown type #{type}"
			end
			if quantity_in_orderbook() == :asset
				size = format_size_str(pair, type, balance, adjust:true, num:true) # Not lower than rules.
				return size
			else
				return balance
			end
		end

		# Callback when balance_cache is really updated in balance_cache_update()
		def balance_changed()
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
		# Don't use this directly, it is designed for _balance_avail() in max_order_size()
		# To get balance:
		# Try reading @balance_cache directly.
		# For placing orders:
		# Try max_order_size(order) instead.
		def pair_to_underlying_pair(pair)
			# Change this for mapping real trading pairs, 
			# only called in _balance_avail(), overwrite this wisely.
			pair
		end
		def underlying_pair_to_pair(pair)
			pair
		end

		# Add trade into memory.
		# Upate balance_cache as well.
		# Very complicated if the trade is already in memory.
		def balance_cache_update(trade, opt={})
			if trade['status'] != 'pending' # Allow estimated trade has no maker_size
				raise "trade should contains maker_size" if trade['maker_size'].nil?
			end
			return balance() if @balance_cache.nil?
			# Use an internal function to avoid deadlock in balance()
			return balance_cache_update_int(trade, opt)
		end
		def balance_cache_update_int(trade, opt={})
			verbose = @verbose && opt[:verbose] != false
			recover_reserved = opt[:recover_reserved] == true
			recover_reserved_deduct_cash = opt[:recover_reserved_deduct_cash] == true
			replace_order = opt[:replace_order] == true
			pair = pair_to_underlying_pair(trade['pair'])

			cancelled = opt[:cancelled] == true
			just_placed = opt[:just_placed] == true
			debug = opt[:debug] == true # debug = (market_type() == :future)

			# If trade timestamp is earlier than balance initialize time, just remember it.
			# These trades do exist before.
			if parse_trade_time(trade['t']) < @balance_cache_init_time
				# Some exchanges (Liqui) does not provide reserved amount in return of balance API.
				# It returns zero reserverd value for non-BTC asset,
				# we should recover it from existed orders.
				# Must use flag recover_reserved to recover missing reserved amount.
				#
				# Some future exchanges Bitmex does not deduct cash from alive orders in balance
				# And it also has missing reserved amount.
				# All of its missing balance and locked cash should be re-computed again.
				# Must use flag recover_reserved_deduct_cash to record them.
        #
				# In Kraken, only total balance is returned, cash needs to be deducted and
				# assets need to be locked.
				# Use flag replace_order in this case
				#
				# Q: What is the difference between recover_reserved_deduct_cash and recover_reserved?
				# A: recover_reserved_deduct_cash also affect base currency cash, just like the order is just placed.
				#    while recover_reserved could only affect underly currency.
				if recover_reserved_deduct_cash
					;
				elsif recover_reserved
					;
				elsif replace_order
					;
				else
					return order_remember(trade, verbose:debug)
				end
			end

			old_trade = order_remember(trade, verbose:debug)
			if old_trade.nil?
				# puts "balance_cache_update_int() new order #{pair} maker_size #{trade['maker_size']}\n#{format_trade(trade)}"
			else # Defensive checking for maker_size
				if just_placed
					raise "just_placed option with an old trade:\n#{format_trade(old_trade)}"
				end
				old_maker_size = old_trade['maker_size']
				# trade from active_orders() and history_orders() has fresh maker_size.
				# This happens in balance() with recover_reserved_deduct_cash == true
				# Overwrite maker size in this case
				if recover_reserved_deduct_cash == true && old_maker_size != trade['maker_size']
					puts "Overwrite maker_size #{old_maker_size} for:\n#{format_trade(trade)}"
					trade['maker_size'] = old_maker_size
				end
				if old_trade['status'] == 'pending' && old_maker_size.nil?
					; # In some place, new placed order is estimated by limit info, might have no maker size.
				elsif trade['status'] == 'pending' && trade['maker_size'].nil?
					trade['maker_size'] = old_maker_size
					puts "trade maker_size recovered from #{old_maker_size}:"
					puts format_trade(trade)
					puts trade.to_json
					raise "trade maker_size changed."
				elsif (quantity_in_orderbook() == :vol && old_maker_size.round(8) != trade['maker_size'].round(8)) || \
						(quantity_in_orderbook() != :vol && old_maker_size != trade['maker_size'])
					# size for vol based market is computed from vol/price
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
			if recover_reserved && old_trade != nil
				puts "Order should be brandly new for recover_reserved, forget it forcely".red
				order_forget(trade)
			end

			# If flag replace_order is set, let maker_size adjusted with old_trade
			# Then we pretend old_trade does not exist to compute full amount cash and asset.
			old_trade = nil if replace_order

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
			asset1, asset2 = pair_assets(pair)
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
					cash_inc1 = 0 - reserved_inc1 unless recover_reserved
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
					cash_inc2 = 0 - reserved_inc2 unless recover_reserved
					if vol_based
						reserved_vol_inc2 = trade['remained_v']
						vol_inc2 = 0 - reserved_vol_inc2 unless recover_reserved
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
				puts ['balance_cache_update1', asset1, cash_inc1.round(8), reserved_inc1.round(8)] if verbose
			end
			if cash_inc2 != 0 && reserved_inc2 != 0
				puts ['balance_cache_update2', asset2, cash_inc2.round(8), reserved_inc2.round(8)] if verbose
			end
			if vol_inc2 != 0 && reserved_vol_inc2 != 0
				puts ['balance_cache_vol_update2', asset2, vol_inc2, reserved_vol_inc2] if verbose
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

			asset2_cache['cash_v'] ||= 0
			asset2_cache['cash_v'] += vol_inc2
			asset2_cache['reserved_v'] ||= 0
			asset2_cache['reserved_v'] += reserved_vol_inc2
			if vol_based
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
							currency, real_asset, expiry = parse_contract(asset)
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
			else
				# Recover tricked order pair to right one, especially for Gemini USDT pairs, USD pair indeed.
				if @trade_mode == 'ab3'
					# Don't need to recover pair yet.
				else
					correct_pair = market_client(order).underlying_pair_to_pair(
						market_client(order).pair_to_underlying_pair(order['pair'])
					)
					if correct_pair != order['pair']
						puts "Order pair #{order['pair']} auto changed to #{correct_pair}".red
						order['pair'] = correct_pair
					end
				end
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

	class RawMarketManager
		include URN::AssetManager
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

	class TooHighLeverage < Exception
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

		def inspect # Avoid too much info in error message
			self.class.name
		end

		def initialize(opt={})
			@lock_mgr = Redlock::Client.new([redis()])
			verbose = opt[:verbose] == true
			silent = opt[:silent] == true
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
			if ['no', 'test', 'default', 'ab3', 'ab3_with_no_deviation', 'bulk', 'algo'].include?(@trade_mode)
				puts "Initializing #{market_name} mode #{@trade_mode}" if @verbose
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

			# Do this before sending API requests.
			URN::MktStatusListener.start_service() # Global unify listener
			@mkt_status_cache ||= Concurrent::Hash.new
			# subscribe_mkt_status() # Separate listener for each market.

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

			# Get stacks, back to 3 levels.
			@task_name = opt[:task_name]
			if @task_name.nil? # Default: stack this client created ab3:567/mkt:814
				stacks = [6,5,4,3].map { |i|
					fline = caller(i).first
					next nil if fline.nil?
					next fline.split(":in")[0].split('/').last.gsub('.rb', '')
				}.select { |s| s != nil }.uniq
				@task_name = stacks.join('/')
			end
		end

		def dispose
			if @market_status_listener.is_a?(Thread)
				status = @market_status_listener.status
				puts "market status listener thread status #{status}"
				if status == false || status.nil?
					;
				else
					@market_status_listener.exit
				end
			end
		end

		def redis
			URN::RedisPool
		end

    def is_ib?
      @_is_ib == true
    end

		def format_transaction_log(tx)
			time_str = format_trade_time(tx['t'])
			type = '??'
			if tx['type'] == 'withdraw'
				type = '->'
			elsif tx['type'] == 'deposit'
				type = '<-'
			end
			addr = tx['address']
			txid = tx['txid']
			finished = tx['finished']
			info = ["#{time_str} #{tx['asset'].ljust(8)} #{type} #{format_num(tx['amount'])}"]
			info.push "\t#{type} #{addr}" if addr != nil
			info.push "\t\tTX: #{txid}" if txid!= nil
			info.push "Not finished" if finished != true
			s = info.join("\n")
			return s if finished == true
			return s.red
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
		def file_cached_call_reset(data_key)
			f = "#{URN::ROOT}/tmp/#{given_name()}_#{data_key}.json"
			FileUtils.rm(f) if File.file?(f)
		end

		def file_cached_call(data_key, opt={})
			raise "API block should be passed" unless block_given?
			f = "#{URN::ROOT}/tmp/#{given_name()}_#{data_key}.json"
			call_first = opt[:call_first] == true
			if call_first == false && File.file?(f)
				return JSON.parse(File.read(f))
			end
			res = yield
			if res.nil?
				raise "No #{f} exist while block call failed" unless File.file?(f)
				# Avoid IO confliction.
				sleep_t = opt[:delay] || (2 + Random.rand(20))
				puts  "Try to load cache from #{f} after #{sleep_t}s"
				keep_sleep sleep_t
				return JSON.parse(File.read(f)) if File.file?(f)
			end
			print "\r--> Save #{f}" unless opt[:silent] == true
			File.open(f, 'w') { |w| w.print res.to_json }
			return res
		end

		# API with cache layers : => Redis => block()
		def redis_cached_call_reset(data_key)
			data_key = "URANUS:CACHE:#{given_name()}:#{data_key}"
			endless_retry { redis.del(data_key) }
		end

		def redis_cached_call(data_key, cache_s, opt={})
			raise "API block should be passed" unless block_given?
			data_key = "URANUS:CACHE:#{given_name()}:#{data_key}"
			print "\r--> redis_cached_call #{data_key}" unless opt[:silent]
			cache = endless_retry { redis.get(data_key) }
			if cache.nil? || cache.empty?
				res = yield
				return nil if res.nil?
				# Cache in redis for 4 hours
				endless_retry { redis.setex(data_key, cache_s, res.to_json) }
			else
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

# 		def subscribe_mkt_status # Use global MktStatusListener instead
# 			return unless @market_status_listener.nil?
# 
# 			@mkt_status_cache ||= Concurrent::Hash.new
# 			_fetch_status()
# 
# 			channel = "URANUS:#{market_name()}:-:status_channel"
# 			@market_status_listener = Thread.new(abort_on_exception:true) {
# 				name = Thread.current[:name] = "#{market_name()}.status_listener"
# 				begin
# 					# puts "<< #{name} subscribing #{channel}"
# 					redis.subscribe(channel) { |on|
# 						on.subscribe { |chn, num|
# 							# puts "<< #{name} subscribed to #{chn} (#{num} subscriptions)"
# 							_fetch_status()
# 						}
# 						on.message { |chn, msg| # Just fetch remote status again once got msg.
# 							# puts "<< #{name} msg #{msg}"
# 							_fetch_status()
# 						}
# 						on.unsubscribe { |chn, num|
# 							raise "Unsubscribed to ##{chn} (#{num} subscriptions)"
# 						}
# 					}
# 				rescue => e
# 					APD::Logger.error e
# 				end
# 			}
# 			puts "Status listener started for #{market_name()} - #{channel}"
# 		end
# 
# 		def _fetch_status
# 			@mkt_status_cache[:ban] = _fetch_banned_info()
# 		end
# 
# 		def _fetch_banned_info
# 			key = "URANUS:#{market_name()}:banned_info"
# 			str = endless_retry(sleep:1) { redis.get(key) }
# 			return nil if str.nil?
# 			j, time, reason = {}, nil, nil
# 			if str[0] == '{'
# 				j = parse_json(str)
# 				time = j['time']
# 				reason = j['reason']
# 			else
# 				j['time'] = time = str
# 			end
# 			t = j['time'] = DateTime.parse(j['time'])
# 			if DateTime.now - t > 30
# 				# Too old to show
# 			else
# 				# Warn info every minute.
# 				if Time.now.to_f - (@_last_banned_print_t || 0) >= 60
# 					puts "<< #{market_name} banned_info got: #{str[0..299]}"
# 					@_last_banned_print_t = Time.now.to_f
# 				end
# 			end
# 			return j
# 		end

		# This is the most frequently called method from redis.
		def banned_util(opt={})
			info = URN::MktStatusListener.banned_util(market_name()) || @mkt_status_cache[:ban]
			return nil if info.nil?
			return info['time']
		end

		def banned_reason
			info = URN::MktStatusListener.banned_util(market_name()) || @mkt_status_cache[:ban]
			return nil if info.nil?
			reason = info['reason']
			if reason != nil && reason.include?('<') && reason.size > 512
				reason = "#{reason[0..50]}...#{reason.size} B HTML"
			end
			return reason
		end

		def set_banned_util(time, reason, opt={})
			key = "URANUS:#{market_name()}:banned_info"
			puts "set banned_util -> #{time} #{key}"
			time ||= '19000101'
			value = {
				'type' => 'ban',
				'market' => market_name(),
				'account' => '-',
				'time'	=> DateTime.parse(time.to_s),
				'task_name'	=> task_name(),
				'reason'	=> reason
			}
			channel = "URANUS:#{market_name()}:-:status_channel"
			channel2 = "URANUS:status_channel"
			if opt[:broadcast].nil? || (opt[:broadcast] == true)
				# Default: broadcast this
				endless_retry(sleep:1) {
					redis.set(key, value.to_json) # Set key-value
					redis.publish(channel, value.to_json) # Notify other subscribers.
					redis.publish(channel2, value.to_json) # Notify other subscribers, unify channel
				}
			else # Set locally.
				URN::MktStatusListener.banned_util_set(market_name(), value)
				@mkt_status_cache[:ban] = value
			end
		end

		# Banned info needs to be clear in time.
		# In every bot cycle this info would be parsed.
		def clear_banned(info='')
			URN::MktStatusListener.clear_banned(market_name())
			@mkt_status_cache.clear
			key = "URANUS:#{market_name()}:banned_info"
			channel = "URANUS:#{market_name()}:-:status_channel"
			channel2 = "URANUS:status_channel"
			puts "clear banned info #{key}"
			value = {
				'type' => 'ban_cleared',
				'market' => market_name(),
				'account' => '-',
				'reason'	=> info
			}
			endless_retry(sleep:1) {
				redis.del(key)
				redis.publish(channel, value.to_json) # Notify other subscribers.
				redis.publish(channel2, value.to_json) # Notify other subscribers, unify channel.
			}
		end

		def is_banned?
			t = banned_util()
			return false if t.nil?
			if t < DateTime.now - 30
				;
			elsif t < DateTime.now
				now = Time.now.to_f
				# Print old banned into every 60 seconds.
				if @_last_banned_print_t != nil && now - @_last_banned_print_t > 60
					puts "#{market_name()} banned until #{t} :#{banned_reason()}"
				end
				@_last_banned_print_t = now
			end
			t > DateTime.now
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
			# puts "#{market_name()} aquire_req_lock #{valid_lock[:resource]} in \##{ct} #{lock_t}ms, hold for #{valid_lock[:validity]}ms"
			return [valid_lock, valid_res, valid_b]
		end

		def unlock_req_lock(lock)
			return if lock.nil?
			no_complain {
				lock_t = Time.now.to_f
				ret = @lock_mgr.unlock(lock)
				lock_t = ((Time.now.to_f - lock_t)*1000).round(2)
				puts "#{market_name()} unlock #{lock[:resource]} in #{lock_t}ms" if lock_t > 1000
				ret
			}
		end

		def release_req_lock(lock, opt={})
			return unless lock.is_a?(Hash)
			lock.unlock()
		end

		######### API Rate limit monitor #########
		# For those has rate controll
		def api_rate_status(opt={})
			api_rate_data = "URANUS:DATA:#{market_name()}_API_RATE"
			screen_width = opt[:width] || terminal_width()
			data = opt[:data]
			if data.nil?
				data = endless_retry { redis.get("#{api_rate_data}_#{opt[:api_idx] || 0}") }
				return [] if data.nil?
				data = JSON.parse(data)
			end
			return [] if data.nil?
			# Render status string
			weight_max_score = data.dig('rule', 'weight')[0]
			weight_timerange = data.dig('rule', 'weight')[1]
			order_max_score = data.dig('rule', 'order')[0]
			order_timerange = data.dig('rule', 'order')[1]
			current_weight_score = 0
			current_order_score = 0
			now_ms = (Time.now.to_f * 1000).to_i
			oldest_weight_timestamp_ms = now_ms - 1000*data.dig('rule', 'weight')[1]
			oldest_order_timestamp_ms = now_ms - 1000*data.dig('rule', 'order')[1]
			oldest_order_rec_ms = nil
			oldest_weight_rec_ms = nil
			data['his'].each { |h|
				if h[0] < oldest_weight_timestamp_ms
					;
				else
					current_weight_score += h[1]
					oldest_weight_rec_ms ||= h[0]
				end
				if h[0] < oldest_order_timestamp_ms
					;
				else
					current_order_score += h[2]
					oldest_order_rec_ms ||= h[0]
				end
			}
			current_weight_score = [current_weight_score, 0].max
			current_order_score = [current_order_score, 0].max
			ratio = current_weight_score/weight_max_score.to_f
			order_width = (screen_width/2).floor-2 # fixed width instead of [order_max_score, 34].max
			weight_width = screen_width - order_width - 1
			if screen_width <= 2*(order_max_score+1)
        order_width = [screen_width/2 - 1, 34].max
				weight_width = screen_width - order_width - 1
			end
			text = "Req #{current_weight_score}/#{weight_max_score} in #{weight_timerange}s #{format_trade_time(oldest_weight_rec_ms).split(' ')[0]} #{market_name()}"
			weight_rate = progressive_string(text, ratio, weight_width)

			ratio = current_order_score/order_max_score.to_f
			text = "Ord #{current_order_score}/#{order_max_score} in #{order_timerange}s #{format_trade_time(oldest_order_rec_ms).split(' ')[0]}"
			order_rate = progressive_string(text, ratio, order_width)
			return [weight_rate, order_rate]
		end

		def monitor_api_rate(opt={})
			self.class.class_eval { include URN::EmailUtil }
			api_rate_data = "URANUS:DATA:#{market_name()}_API_RATE"
			old_data = {}
			shown_text = {}
			last_email_t = 0
			last_ban = is_banned?()
			history = 100.times.map { nil }
			loop {
				screen_width = terminal_width()
				1.times { |idx|
					weight_max_score = nil
					data = endless_retry { redis.get("#{api_rate_data}_#{idx}") }
					if old_data[idx] != data
						old_data[idx] = data

						# 	data = {
						# 		'rule' => {
						# 			'weight' => [@binance_weight_limits['limit'], @binance_weight_limits['second']],
						# 			'order' => [@binance_order_limits['limit'], @binance_order_limits['second']]
						# 		},
						# 		'score' => {
						# 			'weight' => @binance_weight_limits['limit'],
						# 			'order' => @binance_order_limits['limit'],
						# 		},
						# 		'his' => [],
						# 		'extra' => []
						# 	}
						data = JSON.parse(data)

						weight_max_score = data.dig('rule', 'weight')[0]
						order_max_score = data.dig('rule', 'order')[0]
						extra = data['extra'] || [false, 'NULL', 0, false]
						could_call, memo, weight, place_order = extra[0..3]
						weight_str = weight.to_s.ljust(3)
						order_score = data.dig('score', 'order').to_s.rjust(3)
						weight_score = data.dig('score', 'weight').to_s.rjust(4)
						if place_order
							str = " #{weight_score} ORD #{order_score} #{memo}"
						else
							str = " #{weight_score}     #{order_score} #{memo}"
						end
						puts_opt = {}
						head_len = "04/23-21:47:41.4837 bnn:60   + 1   ->   137 ORDER  36 ".size
						if screen_width < 100
							puts_opt = { :no_head => true }
							head_len = "   + 1   ->   137 ORDER  36 ".size
						end
						str_len = screen_width - head_len
						str = str[0..str_len].ljust(str_len+6)
						if could_call == true
							if str.include?("EMG")
								str = "+ #{weight_str} #{str}".magenta
							elsif weight == 0
								str = "+ #{weight_str} #{str}".green
							elsif weight <= 1
								str = "+ #{weight_str} #{str}".blue
							elsif weight <= 2
								str = "+ #{weight_str} #{str}".red
							else
								str = "+ #{weight_str} #{str}"
							end
						else
							str = "XXXXX #{str}"
						end
						history.push str
						history.shift
						puts str, puts_opt
					end

					if weight_max_score.nil? && old_data[idx] != nil
						data = JSON.parse(old_data[idx])
					end
					# Render status string
					shown_text[idx] = api_rate_status(width:screen_width, data:data).join(' ')
				}
				print("\r" + shown_text.values.join(" ").strip)
				keep_sleep 0.05
				# Send email if just banned?
				if is_banned?
					next if last_ban # Dont send duplicated email
					msg = ["#{market_name()} banned util #{banned_util()}"]
					msg += shown_text.values
					msg += history
					msg += shown_text.values
					msg += ['banned_reason', (banned_reason() || 'NULL').gsub('<', '').gsub('>', '')]
					msg = msg.select { |s| s!= nil }.map { |s| s.uncolorize.strip }
					email_receiver = URN::REPORT_RECIPIENT || raise("No email recipient set")
					email_plain email_receiver, msg[0], msg.join("<p>\n"), nil, skip_dup: false
					last_ban = true
				else # Reset banned flag.
					last_ban = false
				end
			}
		end

		######### API Rate limit control #########
		def api_rate_rule
			raise "Not implemented"
# 			{
# 				'rule' => {
# 					'weight' => [9999, 60],
# 					'order' => [100, 10]
# 				},
# 				'score' => {
# 					'weight' => 9999,
# 					'order' => 100
# 				},
# 				'his' => [],
# 				'extra' => []
# 			}
		end

		# Set limit to (new) max with banned message
		# data should be { 'limit' => { type => [MAX_WEIGHT, TIME_RANGE] }, 'his' => [[timestamp, weight, order_weight], ...]] }
		def api_rate_set_max(msg="BANNED", opt={})
			data = api_rate_rule()
			max_weight = data.dig('score', 'weight')
			max_order_weight = data.dig('score', 'order')
			raise "No max api weight" if max_weight.nil?
			raise "No max order api weight" if max_order_weight.nil?
			# Insert a record with max weight.
			now_ms = (Time.now.to_f * 1000).to_i
			data['his'].push([now_ms, max_weight, max_order_weight])
			data['extra'] = [true, msg, max_weight, true]
			key_index = 0
			api_rate_data = "URANUS:DATA:#{market_name()}_API_RATE_#{key_index}"
			endless_retry { redis.set(api_rate_data, data.to_json) }
		end

		# Common API rate control: API key index - 0, weight 1
		def api_rate_control(weight, emergency_call, memo, opt={})
			opt = opt.clone
			return api_rate_control_for_subaccountidx(0, weight, emergency_call, memo, opt)
		end
		# Try best to allow cancel_order API
		# Always allow wss_key API
		# Try best to allow emergency_call
		# Other req: keep 10% emergency quota.
		def api_rate_control_for_subaccountidx(key_index, weight, emergency_call, memo, opt={})
			api_rate_lock = "URANUS:LOCK:#{market_name()}_API_#{key_index}"
			api_rate_data = "URANUS:DATA:#{market_name()}_API_RATE_#{key_index}"
			max_lock_time = 10_000 # ms, for operating api counts
			lock = false
			wait_ct = 1
			loop {
				lock = endless_retry { @lock_mgr.lock(api_rate_lock, max_lock_time) }
				break if lock != false
				# Others are operating counts, should not wait for long time.
				puts "@lock_mgr.lock #{api_rate_lock} again in 1s \##{wait_ct}"
				wait_ct += 1
				keep_sleep 1
			}

			# data should be { 'limit' => { type => [MAX_WEIGHT, TIME_RANGE] }, 'his' => [[timestamp, weight, order_weight], ...]] }
			data = endless_retry { redis.get(api_rate_data) }
			need_reset_data = false
			if data.nil? || data.empty?
				need_reset_data = true
			else
				data = JSON.parse(data)
				expected_data = api_rate_rule()
				# When remote rules differs from expected, reset it.
				if data.dig('rule', 'weight') != expected_data.dig('rule', 'weight')
					need_reset_data = true
				elsif data.dig('rule', 'order') != expected_data.dig('rule', 'order')
					need_reset_data = true
				end
			end
			if need_reset_data
				puts "Reset #{market_name()} API rules".red
				data = api_rate_rule()
			end

			# Trim data from his, update score of weight and order.
			weight_limit = data.dig('rule', 'weight')
			emergency_weight_quota = (0.1 * weight_limit[0]).ceil
			order_limit = data.dig('rule', 'order')
			emergency_order_quota = (0.1 * order_limit[0]).ceil
      if market_name() == 'Binance' # Use more harsh limit on Binance
        emergency_order_quota = (0.15 * order_limit[0]).ceil
      end

			if emergency_call == true # Still keep some quota in case of competetion.
				# Calculation could not be precise so use random number here.
				# When available space is smaller, only very little chance api could_call
				# Keep 3 for parallel competetion.
				if emergency_weight_quota >= 3
					emergency_weight_quota = [Random.rand(emergency_weight_quota), 3].max
				else
					emergency_weight_quota = [Random.rand(emergency_weight_quota), 1].max
				end
				if emergency_order_quota >= 3
					emergency_order_quota = [Random.rand(emergency_order_quota), 3].max
				else
					emergency_order_quota = [Random.rand(emergency_order_quota), 1].max
				end
			elsif opt[:order_priority] != nil && opt[:order_priority] <= 5
				# Tight rules for low priority API req, <= 80%
				emergency_order_quota = (emergency_order_quota * 1.5).ceil
			end

			now_ms = (Time.now.to_f * 1000).to_i
			oldest_weight_timestamp_ms = now_ms - 1000*weight_limit[1]
			oldest_order_timestamp_ms = now_ms - 1000*order_limit[1]
			oldest_boundary_timestamp_ms = [oldest_weight_timestamp_ms, oldest_order_timestamp_ms].min
			place_order = (opt[:place_order] == true)
			his_trimed = 0
			ttl_records = 0
			current_weight_score = 0
			current_order_score = 0
			new_his = []
			data['his'].each { |h|
				ttl_records += 1
				if h[0] < oldest_weight_timestamp_ms
					his_trimed += 1
				else
					new_his.push(h)
				end
				current_weight_score += h[1] if h[0] >= oldest_weight_timestamp_ms
				current_order_score += h[2] if h[0] >= oldest_order_timestamp_ms
			}
			current_weight_score = [current_weight_score, 0].max
			current_order_score = [current_order_score, 0].max
			# puts "#{his_trimed}/#{ttl_records} record trimmed from api rate records" if @verbose
			msg = "API_RATE EMG? #{emergency_call} WEIGHT #{current_weight_score} + #{weight} ORDER #{current_order_score}"

			could_call = false
			over_quota = 0
			if opt[:cancel_order] == true # Try best to allow cancel_order API
				could_call = current_order_score < order_limit[0]
			elsif opt[:wss_key] == true # Always allow wss_key API
				could_call = true
			else 
				# Even emergency_call, the emergency_order_quota exist, just smaller.
				if opt[:place_order] == true
					could_call = (current_order_score < order_limit[0] - emergency_order_quota) && \
						(current_weight_score + weight <= weight_limit[0] - emergency_weight_quota)
					over_quota = current_order_score - (order_limit[0] - emergency_order_quota)
				else
					could_call = (current_weight_score + weight <= weight_limit[0] - emergency_weight_quota)
					over_quota = current_weight_score - (weight_limit[0] - emergency_weight_quota)
				end
			end

			# Update score and prepare message.
			if could_call
				current_weight_score += weight
				current_order_score += 1 if opt[:place_order] == true
			end
			msg += " CALL #{could_call} => #{current_weight_score}"
			msg += " NEWORDER" if opt[:place_order] == true

			# Overquota more -> update less.
			update_on_failed = (Random.rand([20, over_quota+20].max) < 1)
			if could_call == false && update_on_failed == false
				endless_retry { @lock_mgr.unlock(lock) } # Dont forget this.
				return could_call
			end

			if could_call
				# puts msg if @verbose
				if opt[:place_order] == true
					new_his.push([now_ms, weight, 1])
				else
					new_his.push([now_ms, weight, 0])
				end
			else
				puts msg.red
			end
			data['score']['weight'] = current_weight_score
			data['score']['order'] = current_order_score
			data['his'] = new_his
			data['extra'] = [could_call, "#{(@task_name || '').ljust(10)} #{memo}", weight, (opt[:place_order] == true)] # record this call.
			endless_retry { redis.set(api_rate_data, data.to_json) }
			endless_retry { @lock_mgr.unlock(lock) }

			return could_call
		end

    def is_ib?
      (@_is_ib == true)
    end

		def is_transferable?
			false
		end

		def can_deposit?(asset, opt={})
			true
		end

		def can_withdraw?(asset, opt={})
			true
		end

		def support_tron?(asset='USDT')
			false
		end

		def test_tron_network(asset)
			asset = asset.upcase
			a = support_tron?(asset)
      b = deposit_addr(asset, network: 'TRON', allow_fail: true)
      c = withdraw_fee(asset, network: 'TRON', allow_fail: true)
      d = withdraw_fee(asset, allow_fail: true)
      e = can_deposit?(asset, network: 'TRON')
      f = can_withdraw?(asset, network: 'TRON')
			puts "------------- #{asset} TRC20 Test ------------"
			if a
				puts "support_tron? #{a}".green
			else
				puts "support_tron? #{a}".red
			end
			if b != nil && b.start_with?('T')
				puts "deposit_addr(#{asset}, network:TRON) #{b}".green
			else
				puts "deposit_addr(#{asset}, network:TRON) #{b}".red
			end
			if c != nil && d != nil && c < d
				puts "withdraw_fee(#{asset}, network:TRON) #{c}".green
			else
				puts "withdraw_fee(#{asset}, network:TRON) #{c}".red
			end
			puts "withdraw_fee(#{asset}) #{d}"
			if e
				puts "can_deposit?  #{e}".green
			else
				puts "can_deposit?  #{e}".red
			end
			if f
				puts "can_withdraw? #{f}".green
			else
				puts "can_withdraw? #{f}".red
			end
		end

		def withdraw_limit(asset, opt={})
			nil
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
      if @_is_ib # Use IB module to query and return real pair.
        contracts = _ib_match_contracts(ib_exchange(), pair)
        raise "Could not find exact contract #{ib_exchange()} #{pair}:#{JSON.pretty_generate(contracts)}" unless contracts.size == 1
        return _ib_contract_to_pair(contracts.first)
      end
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
			elsif pairs.include?(pair) # Exactly match
				return pair
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
			# Validation of contract pairs.
			# If pair@expiry is expired, return recent active one.
			return nil if pair.nil?
			raise "Only for future market" unless market_type() == :future
			if pair.include?('@')
				if all_pairs()[pair].nil?
					spot_pair, expiry = pair.split('@')
					if expiry == 'P'
						raise "No PERP pair #{pair}"
					else
						return contract_name(spot_pair)
					end
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
			raise "Should not come here any more #{market_name()}"
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
			if market_name() == 'FTX' && is_future?(pair) == false && market_type() != :spot
				ftx_enter_spot_mode()
			end

			return preprocess_deviation_evaluate_for_futures(pair, opt) if market_type() == :future
			pair = get_active_pair(pair)
			map = fee_rate_real_evaluate(pair, opt).clone()
			return nil if map.nil? && opt[:allow_fail] == true
			if trade_mode() != 'ab3'
				@_preprocess_deviation ||= {}
				@_preprocess_deviation[pair] = map
				return
			end
			currency, asset = pair_assets(pair)
			sell_deviation_adjust = false
			if can_withdraw?(asset) == false || market_name() == 'HitBTC' # Stop buying in HitBTC
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

			if high_withdraw_fee_deviation(currency) != nil && !sell_deviation_adjust && market_name() != 'HitBTC' # Sell all in HitBTC
				dev = high_withdraw_fee_deviation(currency)
				# Currency has high withdraw fee, add deviation for selling pair.
				map['maker/sell'] += dev
				map['taker/sell'] += dev
			end

			map = map.to_a.map { |kv| [kv[0], kv[1].round(10)] }.to_h
			@_preprocess_deviation ||= {}
			@_preprocess_deviation[pair] = map
		end

		# Default behavior:
		# as same as fee_rate_real_evaluate()
		# --------------------------------------
		# In trade_mode ab3:
		# Check position first,
		# * if holding zero position, set big deviation (4%) for both side (buy/sell)
		# * If holding a small position, to avoid changing deviation often,
		# 	don't need to be hurry to close position with negative deviation,
		# 	switch to a small positive deviation (1%) to close it.
		# * If holding a relatively big position, switch to negative deviation (-1%) for closing.
		#
		# Check expiry:
		# When expiry date is getting close, make the bar lower for big deviation:
		# For quarterly future, decrease by 25% per month
		# For monthly future, decrease by 25% per week
		#
		# NOTE Disable negative deviation in DEBUG MODE
		#
		# TODO Also work with future_max_position_cost():
		# Bigger deviation comes with bigger future_max_position_cost()
		# --------------------------------------
		def preprocess_deviation_evaluate_for_futures(pair, opt={})
			pair = get_active_pair(pair)
			@_preprocess_deviation ||= {}
			old_deviation = @_preprocess_deviation[pair]
			map = fee_rate_real_evaluate(pair, opt).clone()
			# Only works for mode ab3
			if trade_mode() != 'ab3'
				@_preprocess_deviation[pair] = map
				return
			end
			# TODO force refresh balance before evaluating?
			balance() if @balance_cache.nil?
			position = future_position(pair)
			position_cost = future_position_cost(pair)
			# In case of absent future_max_position_cost()
			raise "#{pair} position is not zero but cost is zero" if position != 0 && position_cost == 0
			position_type = (position >= 0) ? 'buy' : 'sell'
			max_position_cost = future_max_position_cost(pair, position_type)
			position_is_small = position_cost.to_f.abs/max_position_cost < 0.1
			position_is_large = position_cost.to_f.abs/max_position_cost > 0.9
			if pair.end_with?('@P')
				days_remain = nil
			else
				days_remain = Date.parse(pair.split('@')[1])- Date.today
			end

			buy_deviation = nil
			sell_deviation = nil
			# Normally base interest rate is 1% per 30 days.
			if pair.end_with?('@P') # TODO dynamic with funding rate
				buy_deviation = { 'open' => 0.00 }
				sell_deviation = { 'open' => 0.00 }
			elsif future_expiry_phase(pair, days_remain) == 0 || future_expiry_phase(pair, days_remain) == 1
				buy_deviation = { 'open' => 0.01 }
				sell_deviation = { 'open' => 0.04 }
			elsif future_expiry_phase(pair, days_remain) == 2 || future_expiry_phase(pair, days_remain) == 3
				buy_deviation = { 'open' => 0.01 }
				sell_deviation = { 'open' => 0.03 }
			elsif future_expiry_phase(pair, days_remain) == 4 || future_expiry_phase(pair, days_remain) == 5
				buy_deviation = { 'open' => 0.01 }
				sell_deviation = { 'open' => 0.01 }
				if position_is_small
					buy_deviation['close'] = sell_deviation['close'] = 0
				end
			elsif future_expiry_phase(pair, days_remain) == 6
				# High deviation to stop opening position
				buy_deviation = { 'open' => 0.10, 'close' => -0.002 }
				sell_deviation = { 'open' => 0.10, 'close' => -0.002 }
			end

			# Auto fill close deviation.
			if pair.end_with?('@P') # TODO dynamic with funding rate
				buy_deviation['close'] = 0.0
				sell_deviation['close'] = 0.0
			elsif position == 0
				;
			elsif position_is_small
				buy_deviation['close'] ||= sell_deviation['open']
				sell_deviation['close'] ||= buy_deviation['open']
			else
				buy_deviation['close'] ||= buy_deviation['open']/-4.0
				sell_deviation['close'] ||= sell_deviation['open']/-4.0
			end

			add_deviation = {}
			['maker', 'taker'].each do |t1|
				if position == 0
					add_deviation["#{t1}/buy"] = buy_deviation['open']
					add_deviation["#{t1}/sell"] = sell_deviation['open']
				elsif position > 0 # Long
					add_deviation["#{t1}/buy"] = buy_deviation['open']
					add_deviation["#{t1}/sell"] = buy_deviation['close']
				else # Short
					add_deviation["#{t1}/sell"] = sell_deviation['open']
					add_deviation["#{t1}/buy"] = sell_deviation['close']
				end
			end
			['maker', 'taker'].each do |t1|
					['buy', 'sell'].each do |t2|
						v = add_deviation["#{t1}/#{t2}"]
						raise "Invalid deivation data #{add_deviation}" if v.nil?
						map["#{t1}/#{t2}"] += v
					end
			end
			map = map.to_a.map { |kv| [kv[0], kv[1].round(10)] }.to_h
			if old_deviation != map
				puts "Deviation #{pair} changed from:\n#{old_deviation}\nto:\n#{map}"
				puts "#{days_remain} days left, position: #{position}"
				puts "Position cost #{position_cost} Max cost #{max_position_cost} pos_small?:#{position_is_small}"
				puts "add_deviation:#{add_deviation}"
			end
			@_deviation_compute_t = Time.now.to_f
			@_preprocess_deviation[pair] = map
		end

		# Determine future pair phase
		# 0 -> Reserved
		# For long term futures (days_remain >= 16, quarterly):
		# 1 -> Expiry in 70+ days
		# 2 -> Expiry in 20~69 days
		# 3 -> Reserved
		# 4 -> Reserved
		# For short term futures (days_remain < 16, weekly):
		# 5 -> Expiry in 2~19 days
		# 6 -> Expiry in 2 days
		def future_expiry_phase(pair, days_remain)
			if days_remain >= 200
				raise "days_remain too large #{days_remain} maybe an error? #{pair}"
			elsif days_remain >= 70
				return 1
			elsif days_remain >= 20
				return 2
			elsif days_remain >= 2
				return 5
			else
				return 6
			end
		end

		def chart_string_for_funding_rate_history(pair, x, opt={})
			his = funding_rate_stat(pair, x, opt)
			return '' if his.nil? && opt[:allow_fail] == true
			rate_records = his['rate_apy'] # From latest to oldest
			rate_records = rate_records.map { |r| r.to_f/100 }
			if market_name() != 'FTX'
				rec_by_hour = []
				rate_records.each { |a| 8.times { rec_by_hour.push a }}
				rate_records = rec_by_hour
			end
			chart_string(rate_records)
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

		def price_ratio_range(pair)
			[0, 99999]
		end

		def price_in_valid_range(pair, current_p, order)
			range = price_ratio_range(pair)
			return false if order['T'] == 'buy' && order['p'] < range[0]*current_p
			return false if order['T'] == 'sell' && order['p'] > range[1]*current_p
			return true
		end

		def price_precision(pair)
			pair = get_active_pair(pair)
			min_price_step = price_step(pair)
			precision = 0
			loop do
        break if min_price_step.to_s.to_i == min_price_step
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
		def secondary_quantity_step(pair)
			quantity_step(pair)
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
			raw_price = price
			if type != nil
				price = format_price_str(pair, type, price, adjust:true, num:true)
			end
			if price <= 0
				puts "Price step of #{market_name()} #{get_active_pair(pair)} is #{price_step(get_active_pair(pair))}" # DEBUG
				error = "price invalid #{pair} #{raw_price} -> #{price} #{type} #{order}"
        if price == 0 && order['T'] == 'buy'
          puts error.red
          puts "Use price_step instead.".red
          price = price_step(get_active_pair(pair))
        else
          raise error
        end
			end
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
      multiplier = opt[:multiplier] || 1
			size = size.to_f if size.is_a?(BigDecimal)
			# Must check class to avoid string*bignum
			raise "size should be a num: #{size}" if size.class != 1.class && size.class != (1.1).class
			if quantity_in_orderbook() == :asset
				toint = 10000000000 # 10^11 is precise enough.
				step = quantity_step(pair)
        step *= multiplier
				raise "step should be a num: #{step}" if step.class != 1.class && step.class != (1.1).class
        step_i = (step * toint).round
        size_i = (size * toint).round
				new_size_i = size_i / step_i * step_i
				if new_size_i != size_i
					raise "Size #{format_num(size)} should be integer times of step: #{format_num(step)}" if opt[:adjust] != true
					puts "size #{size_i}->#{new_size_i} stp:#{step_i}" if verbose
					size = new_size_i.to_f/toint.to_f
				end
        size /= multiplier
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
			logs = []
			head = opt[:head] || 'cache'
			assets = opt[:assets] # Customized filter
			s = "Balance [#{head}] - " + market_name()
			logs.push s
			if market_type() == :future
				s = "#{'Bal'.ljust(16)} #{format_num('CASH', 8)} #{format_num("CASH_V", 8)} #{format_num('RESERVED', 8)} #{format_num('PENDING', 4)}"
				logs.push s
				@balance_cache.each do |k, v|
					next if assets != nil && assets.include?(k) == false
					s = "#{(k||'N/A').ljust(24)} #{format_num(v['cash'], 8)} #{(v['cash_v'] || '').to_s.rjust(8)} #{format_num(v['reserved'], 8)} #{format_num(v['pending']||0, 4)}"
					logs.push s
				end
			else
				s = "#{'Bal'.ljust(16)} #{format_num('CASH', 8)} #{format_num('RESERVED', 8)} #{format_num('PENDING', 4)}"
				logs.push s
				@balance_cache.each do |k, v|
					next if assets != nil && assets.include?(k) == false
					s = "#{(k||'N/A').ljust(24)} #{format_num(v['cash'], 8)} #{format_num(v['reserved'], 8)} #{format_num(v['pending']||0, 4)}"
					logs.push s
				end
			end
			logs.each { |s| print "#{s}\n" }
			logs
		end

		def balance_mv(opt={})
			ret = balance(opt={})
			return nil if ret.nil? && opt[:allow_fail] == true
			mv_map = {}
			price_in_b_map = {}
			pairs = all_pairs(opt)
			return nil if pairs.nil? && opt[:allow_fail] == true
			pairs = pairs.keys
			@balance_cache.each { |asset, v|
				v = @balance_cache[asset]
				bal = (v['reserved'] || 0) + (v['cash'] || 0) + (v['pending'] || 0)
				next if bal == 0
				next if asset == 'BTC'
				price = 0
				opt[:verbose] = false
				if pairs.include?("#{asset}-BTC")
					his = market_summary("#{asset}-BTC", opt)
					return nil if his.nil? && opt[:allow_fail] == true
					puts [asset, his]
					price = 1.0/(his['last'].to_f)
				elsif pairs.include?("BTC-#{asset}")
					his = market_summary("BTC-#{asset}", opt)
					return nil if his.nil? && opt[:allow_fail] == true
					puts [asset, his]
					price = 1.0/(his['last'].to_f)
					price = (his['last'].to_f)
				elsif pairs.include?("USDT-#{asset}")
					his = market_summary("USDT-#{asset}", opt)
					return nil if his.nil? && opt[:allow_fail] == true
					price_in_u = his['last'].to_f
					if pairs.include?("USDT-BTC")
						his = market_summary("USDT-BTC", opt)
					elsif pairs.include?("USD-BTC")
						his = market_summary("USD-BTC", opt)
					else
						raise "No USDT-BTC or USD-BTC in all pairs"
					end
					return nil if his.nil? && opt[:allow_fail] == true
					price = 1/his['last'].to_f * price_in_u
				else
					puts "Could not determine price of #{asset}".red
				end
				price_in_b_map[asset] = price
			}
			price_in_b_map['BTC'] = 1.0
			logs = []
			logs.push "#{'ASSET'.ljust(8)} #{market_name()} amount, MV and fee".white.on_blue
			mv_ttl = 0
			info = {}
			@balance_cache.keys.sort.each_with_index  { |asset, line_ct|
				v = @balance_cache[asset]
				bal = (v['reserved'] || 0) + (v['cash'] || 0) + (v['pending'] || 0)
				next if bal == 0
				fee = withdraw_fee(asset, opt)
				return nil if fee.nil? && opt[:allow_fail]
				can_withdraw = can_withdraw?(asset)
				return nil if can_withdraw.nil? && opt[:allow_fail]

				p = price_in_b_map[asset] || 0
				mv = (bal*(price_in_b_map[asset] || 0)*1000).round
				next if mv == 0
				# puts [asset, bal, p, mv, fee]
				mv_ttl += mv
				s = "#{asset.ljust(6)} #{format_num(bal, 4, 8)} #{mv.to_s.rjust(6)}"
				s = s.blue if line_ct % 2 == 0
				if can_withdraw && bal/fee > 1500
					s = s + ' √ '.on_green
					s = s + " #{fee}"
				elsif can_withdraw == false || fee >= 99999
					s = s + ' x '.red
				else
					s = s + ' √ '
					s = s + " #{fee}"
				end
				info[asset] = { :bal => bal, :fee => fee, :can_withdraw => can_withdraw }
				logs.push s
			}
			logs.push "#{'TTL'.ljust(8)} #{format_num(0)} #{mv_ttl.to_s.rjust(6)} mBTC".red.on_white
			logs.each { |l| print "#{l}\n" }
			return {
				:mv => mv_ttl/1000.0,
				:info => info,
				:logs => logs
			}
		end

		def normal_api_error?(e)
			return false if e.nil?
			return true if e.is_a?(SOCKSError) # From Socksify warpped Net::HTTP
			return true if e.is_a?(HTTP::ConnectionError)
			return true if e.is_a?(HTTP::TimeoutError)
			return true if e.is_a?(Timeout::Error)
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
			puts "order_args: #{order_args}"
			puts "additional opt: #{opt}"
			puts "part of recent_orders:"
			recent_orders.
				select { |o| o['pair'] == pair }.
				uniq { |o| o['i'] }.
				sort_by { |o| o['t'].to_i }.reverse.
				select { |o| order_cancelled?(o) == false }.
				select { |o| @_latest_placed_order_ids.include?(o['i']) == false }.
				select { |o| o['T'] == type }.
				each { |o| puts format_trade(o) }
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
			dup_orders.each { |o| puts format_trade(o) }
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

		def set_order_limit(pair, type, key, value)
			@fuse_order_limit ||= {}
			@fuse_order_limit[pair] ||= {}
			key = key.to_s
			if type.nil?
				@fuse_order_limit[pair]['buy'] ||= {}
				@fuse_order_limit[pair]['buy'][key] = value
				@fuse_order_limit[pair]['sell'] ||= {}
				@fuse_order_limit[pair]['sell'][key] = value
			else
				@fuse_order_limit[pair][type] ||= {}
				@fuse_order_limit[pair][type][key] = value
			end
		end

		def pre_place_order(pair, order)
			return false if is_banned?()
			pair = get_active_pair(pair)
			# Last minute check.
			raise "Error trade mode #{trade_mode()}" if trade_mode() == 'no'
			['i', 'client_oid'].each { |k|
				v = order[k]
				raise "Placing order already contains #{k}=#{v}" unless v.nil?
			}
			# Limitation check
			max_size = (@fuse_order_limit || {}).dig(pair, order['T'], 'size')
			max_vol = (@fuse_order_limit || {}).dig(pair, order['T'], 'vol')
			if max_size != nil && (order['s'] || (order['v'] / order['p'])) > max_size
				raise "Order size beyond limitation #{max_size} #{order}"
			end
			if max_vol != nil && (order['v'] || (order['s'] * order['p'])) > max_vol
				raise "Order vol beyond limitation #{max_vol} #{order}"
			end

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
      # Print balance for debugging.
      currency, asset = pair_assets(pair_to_underlying_pair(pair))
      bal_c = @balance_cache.dig(currency, 'cash')
      bal_a = @balance_cache.dig(asset, 'cash')
			puts "Place #{pair} order: #{format_order(order)}\nBalance #{currency}: #{bal_c}, #{asset}: #{bal_a}", level:2
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

		def oms_process_running?
			oms_running_key = "URANUS:#{market_name()}:#{account_name()}:OMS"
			oms_running = endless_retry(sleep:1) { redis.get(oms_running_key) }
			puts [oms_running_key, oms_running]
			return false if oms_running.nil?
			return true
		end

		def oms_balance(opt={})
      hash_name = "URANUS:#{market_name()}:-:P:#{market_type()}"
			ret = endless_retry(sleep:1) { redis.hgetall(hash_name) }
			if ret.nil? || ret.empty?
				puts "No data in hmap #{hash_name}".red
				return nil
			end
			now = Time.now.to_f * 1000
			oms_t = ret.delete('t').to_i
			oms_t_age = (now - oms_t).to_i
			if oms_t_age >= 600_000 # 10 minutes
				puts "Too old data in hmap #{hash_name}, #{oms_t_age} ms ago".red
				return nil
			end
			ret.keys.each { |k|
				ret[k] = JSON.parse(ret[k]) if ret[k].is_a?(String)
			}
			puts "#{ret.size} records got from #{hash_name}" unless opt[:silent] == true
			ret
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
			puts ">> #{hash_name} #{id_list}" if verbose
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
          if opt[:loop] != nil && opt[:loop] % 50 == 0
            puts "<< OMS #{hash_name} has no #{id_list} \##{opt[:loop]}"
          else # Too much logs
            puts "<< OMS #{hash_name} has no #{id_list}" if verbose
          end
					return nil
				end
				# Make sure OMS cache contains info, now check the maintained timestamp.
				# OMS should clear t after quit or crashed.
				# If oms_order_write_if_null() done, t might still be nil for rare active pairs.
				# Check if OMS running.
				# Clear old cache if OMS is not running. Maybe OMS crashed without delete status.
				if t.nil? && (URN::OMSLocalCache.support_mkt?(market_name()) == false)
					# If OMSLocalCache.support_mkt, do not need to check oms_running_key
					oms_running_key = "URANUS:#{market_name()}:#{account_name()}:OMS"
					oms_running = endless_retry(sleep:1) { redis.get(oms_running_key) }
					if oms_running.nil?
						puts "<< OMS no t for #{hash_name}, OMS OFF".red # if verbose
						oms_order_delete(pair, id)
						return nil
					else # Treat info as valid data.
						# puts "<< OMS no t for #{hash_name}, OMS #{oms_running}", inline:true
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
					puts "<< OMS all results empty" if is_banned?() == false # Only warn if needed
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
			puts ">> write to OMS/#{pair} #{id} -> #{ret}".blue
			if URN::OMSLocalCache.support_mkt?(market_name()) # Must update OMSLocalCache
				URN::OMSLocalCache.oms_set_if_null(market_name(), id, trade.to_json)
			end
		end

		def oms_order_delete(pair, id)
			if oms_enabled? == false
				puts "OMS disabled, skip oms_order_delete()".red # Warn mildly.
				return nil
			end
			hash_name = "URANUS:#{market_name()}:#{account_name()}:O:#{pair}"
			ret = endless_retry(sleep:1) { redis.hdel(hash_name, id) }
			puts ">> Delete OMS/#{pair} #{id} -> #{ret}".red
			if URN::OMSLocalCache.support_mkt?(market_name()) # Must update OMSLocalCache
				URN::OMSLocalCache.oms_delete(market_name(), id)
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
      oms_scan_orders(pair, :active, opt)
    end
		def oms_all_orders(pair, opt={})
      oms_scan_orders(pair, :all, opt)
    end
		def oms_scan_orders(pair, mode, opt={})
			if oms_enabled? == false
				puts "OMS disabled".red # Warn mildly.
				return nil
			end
			return oms_scan_orders_by_pair(pair, mode, opt) if pair != nil
			verbose = @verbose && opt[:verbose] != false
			# Get all pairs then all orders
			prefix = "URANUS:#{market_name()}:#{account_name()}:O:"
			hash_names = endless_retry(sleep:1) {
				redis.keys('URANUS*').select { |n| n.start_with?(prefix) }
			}
			orders = []
			hash_names.each do |n|
				p = n.split(':').last
				pair_orders = oms_scan_orders_by_pair(p, mode, opt)
				next if pair_orders.nil?
				pair_orders.each { |o|
					print "#{o['pair']}\n" if verbose && pair.nil?
					print "#{format_trade(o)}\n" if verbose
				}
				orders += pair_orders
			end
			orders
		end

		def oms_scan_orders_by_pair(pair, mode, opt)
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
      latest_orders = order_map.values.map { |j| _normalize_trade(pair, JSON.parse(j)) }.select { |o|
        if mode == :active
          next order_alive?(o)
        else
          next true
        end
      } .uniq { |o| o['i'] }
      if verbose
        if mode == :active
          puts "<< OMS #{pair} #{latest_orders.size}/#{order_map.size} alive orders"
        else
          puts "<< OMS #{pair} #{latest_orders.size}/#{order_map.size} uniq  orders"
        end
        latest_orders.each { |o| print "#{format_trade(o)}\n" }
      end
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
      verbose = (opt[:verbose] != false)
			trade = nil
			loop_start_t = Time.now.to_f
			wait_ct = 0

			oms_cache_supported = URN::OMSLocalCache.support_mkt?(market_name())
			URN::OMSLocalCache.add_listener(Thread.current) if oms_cache_supported
			puts "#{market_name()} oms_cache_supported #{oms_cache_supported}" if verbose

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
					elsif mode == :query # same as :new/:query_new , just not print too much
						puts "Order #{id} found \##{wait_ct}" if verbose
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
			return false if is_banned?()
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
				exception_expected = false
				# Only in bitstamp:
				# If placed order is pretty new and zero filled, its price might be changed better because of latency, longest time seen: 287s.
				if market_name() == 'Bitstamp' && order['executed'] == 0 && trade['executed'] > 0 && order_age(order) < 360_000 && (order['T'] || trade['T']) == trade['T'] && order['p'] != nil
					if order['T'] == 'buy' && trade['p'] < order['p']
						exception_expected = true
					elsif order['T'] == 'sell' && trade['p'] > order['p']
						exception_expected = true
					end
				end
				unless exception_expected
					puts order.to_json
					puts trade.to_json
					puts ((order['executed'] || trade['executed']) > trade['executed'])
					puts (order['p'] || trade['p']).round(10) != trade['p'].round(10)
					puts ((order['T'] || trade['T']) != trade['T'])
					raise "Unconsistent order:\n#{format_trade(order)}\n#{format_trade(trade)}"
				end
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

		# Cancel all related orders to prepare enough balance.
		def prepare_enough_balance(asset, amount, opt={})
			from_mkt = market_name()
			ftx_enter_spot_mode() if from_mkt == 'FTX'
			asset = asset.upcase
			opt[:allow_fail] = true if opt[:allow_fail].nil? # This is not a critical action

			# Cancel all related orders if xfr amount < available*1.1
			cancel_order_before = false
			asset_bal = balance(opt) || @balance_cache
			if asset_bal.nil? && opt[:allow_fail] == true
				puts "Failed in querying balance for #{from_mkt}".red
				return nil
			elsif asset_bal.dig(asset, 'cash') > amount * 1.1
				puts "#{from_mkt} has #{asset_bal.dig(asset, 'cash')}, dont need to cancel orders".green
				cancel_order_before = false
			elsif asset_bal.dig(asset).values.map { |v| v||0 }.sum < amount
				puts "Asset balance total is lower than #{amount} #{asset_bal[asset]}, abort".red
				return nil
			else
				puts "#{from_mkt} has #{asset_bal.dig(asset, 'cash')}, need to cancel related orders".red
				cancel_order_before = true
			end
			puts "to prepare #{amount} #{asset} cancel_order_before #{cancel_order_before}".red
			if cancel_order_before
				pairs = all_pairs().keys.select { |p| p.include?("-#{asset}") }
				based_pairs = all_pairs().keys.select { |p| p.include?("#{asset}-") }
				could_be_base = (URN::USD_TOKENS + ['BTC', 'USD', 'ETH', 'TRX']).include?(asset)
				# Ask all related bots to pause 30 or 90 seconds.
				command = nil
				if asset == 'BNB'
					puts "Do nothing for BNB, don't pause markets.".green
				elsif could_be_base
					puts "Sending URANUS:command --> pause90 #{asset}".red # pause90 USDT
					command = [Time.now.to_i, "pause60 #{asset}-"].to_json
				else
					puts "Sending URANUS:command --> pause30 #{asset}".red # pause30 DOGE
					command = [Time.now.to_i, "pause30 -#{asset}"].to_json
				end

				if command != nil
					endless_retry(sleep:1) { redis().set("URANUS:command", command) }
				end
				# Cancel all related orders.
				if (pairs + based_pairs).uniq.size <= 5
					# Cancel by pair
					(pairs + based_pairs).uniq.sort.each { |pair|
						puts "Getting #{from_mkt} #{pair} orders"
						orders = active_orders(pair, allow_fail: true)
						if orders.nil?
							puts "Failed in getting #{from_mkt} #{pair} orders, hope this does not affect".red
						else
							if pairs.include?(pair)
								orders = orders.select { |o| o['T'] == 'sell' }
							elsif based_pairs.include?(pair)
								orders = orders.select { |o| o['T'] == 'buy' }
							end
							ret = cancel_orders(pair, orders, allow_fail: true)
							if ret.nil?
								puts "Failed in canceling #{from_mkt} #{pair} orders, hope this does not affect".red
							end
						end
					}
				else
					# Fetch all orders, filter by pairs
					orders = active_orders(nil)
					orders_by_pair = {}
					orders = orders.select { |o|
						if pairs.include?(o['pair']) && o['T'] == 'sell'
							orders_by_pair[o['pair']] ||= []
							orders_by_pair[o['pair']].push o
						elsif based_pairs.include?(o['pair']) && o['T'] == 'buy'
							orders_by_pair[o['pair']] ||= []
							orders_by_pair[o['pair']].push o
						end
					}
					orders_by_pair.each { |pair, orders|
						ret = cancel_orders(pair, orders, allow_fail: true)
						if ret.nil?
							puts "Failed in canceling #{from_mkt} #{pair} orders, hope this does not affect".red
						end
					}
				end
			end
		end

		def margin_status(opt={})
			balance() if @balance_cache.nil?

			all_assets = []
			if opt[:asset].nil?
				@balance_cache.each { |contract, d|
					next unless is_future?(contract)
					all_assets.push(future_margin_asset(contract))
				}
				all_assets.uniq!
			else
				all_assets = [opt[:asset].upcase]
			end

			result = []
			all_assets.each { |asset|
				margin_wallet_cash = @balance_cache.dig(asset, 'cash') || 0
				margin_wallet_pnl = @balance_cache.dig(asset, 'pending') || 0
				margin_wallet_bal = margin_wallet_cash + margin_wallet_pnl
				# For FTX, use USD * 10 as max of margin_wallet_bal
				if market_name() == 'FTX' && asset == 'FTX_COLLATERAL'
					usd_wallet_cash = @balance_cache.dig('USD', 'cash') || 0
					if usd_wallet_cash > 0
						margin_wallet_bal = [margin_wallet_bal, usd_wallet_cash*10].min
					else
						margin_wallet_bal = 1
					end
				end
				position = 0
				position_v = 0
				contracts = {}
				max_lev = []
				diversity_mul_by_contract = {}
				@balance_cache.each { |contract, d|
					next unless is_future?(contract) && future_margin_asset(contract) == asset
					contracts[contract] = d
					if position == 0
						position += (d['cash'] + d['reserved'])
						if quantity_in_orderbook() == :asset # USDT for USDT-BTC@P
							position_v += (d['cash_v'] + d['reserved_v'])
						end
					else # Dont merge long and short pos
						position = position.abs + (d['cash'] + d['reserved']).abs
						if quantity_in_orderbook() == :asset
							if d['cash'] + d['reserved'] == 0
								# In the case only order is placed, no cash_v and reserved_v is provided.
								position_v = position_v.abs
							else
								raise "No cash_v for #{contract} balance #{d}" if d['cash_v'].nil?
								raise "No reserved_v for #{contract} balance #{d}" if d['reserved_v'].nil?
								position_v = position_v.abs + (d['cash_v'] + d['reserved_v']).abs
							end
						end
					end
					if d['cash'] + d['reserved'] > 0
						max_lev.push(future_max_long_leverage(contract))
						diversity_mul_by_contract[contract] = future_diversity_mul(contract)
					else
						max_lev.push(future_max_short_leverage(contract))
						diversity_mul_by_contract[contract] = future_diversity_mul(contract)
					end
				}
				r = {
					'asset' => asset,
					'position_sum' => position,
					'position_v_sum' => position_v,
					'wallet_balance'	=> margin_wallet_bal,
					'contracts' => contracts.keys,
					'raw' => contracts.values,
					'max_lev' => max_lev.min || 1.0,
					'diversity_mul_by_contract' => diversity_mul_by_contract,
					'diversity_mul' => future_diversity_mul()
				}
				if quantity_in_orderbook() == :asset
					r['position_sum'] = nil
					next if position_v == 0 && margin_wallet_bal == 0
					r['risk_balance'] = position_v
					r['lev'] = position_v.abs/margin_wallet_bal
				else
					r['position_v_sum'] = nil
					next if position == 0 && margin_wallet_bal == 0
					r['risk_balance'] = position
					r['lev'] = position.abs/margin_wallet_bal
				end
				# Prepare a brief title.
				title = contracts.keys.map { |contract|
					# USD-BTC@20200925 => BTC0925
					# USD-BTC@P => BTC@P
					asset1, asset2, expiry = parse_contract(contract)
					if expiry.size > 4
						next "#{asset2}#{expiry[4..-1]}"
					else
						next "#{asset2}@#{expiry}"
					end
				}.join(',')
				r['name'] = title

				result.push r
			}

			return result
		end

		def margin_status_desc(opt={})
			length = opt[:length]
			margin_status(opt).map { |r|
				r['desc'] unless r['desc'].nil?
				lines = []

				name = r['name'] || '??'
				name_in_separate_line = false
				max_name_in_line_length = 7
				max_name_in_line_length = 17 if r['asset'] == 'USDT'
				if length != nil && name.size > max_name_in_line_length
					# Prepare separate lines for name
					name_in_separate_line = true
					name_segs = name.split(',')
					lines.push(name_segs[0])
					name_segs.each_with_index { |n, i|
						next if i == 0
						last_line = lines.last
						if last_line.size + n.size < length - 1
							lines.push(lines.pop + ' ' + n)
						else
							lines.push n
						end
					}
				end

				if r['asset'] == 'USDT' || r['asset'] == 'FTX_COLLATERAL'
					line = [
						(name_in_separate_line ? 'ALL' : name).rjust(max_name_in_line_length),
						rough_num(r['risk_balance']).to_s.rjust(7),
						'/',
						rough_num(r['wallet_balance']).to_s.rjust(7),
						'$',
						'=',
						"#{(r['lev']*100).round}%".rjust(5),
					].join(' ')
				else
					line = [
						(name_in_separate_line ? 'ALL' : name).rjust(max_name_in_line_length),
						rough_num(r['risk_balance']).to_s.rjust(11),
						'/',
						rough_num(r['wallet_balance']).to_s.rjust(8),
						(r['asset'] || '??').rjust(5),
						'=',
						"#{(r['lev']*100).round}%".rjust(5),
					].join(' ')
				end
				lines.push line
				all_line_length = length || line.size
				lines = lines.map { |l|
					progressive_string(l, r['lev']/r['max_lev'], all_line_length, color: 'magenta')
				}
				r['desc'] = lines
				next lines
			}.reduce(:+) || []
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
			pair = nil
			order_args = nil
			if market_name() == 'HBDM'
				pair = 'USD-BTC'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>20000.0,'s'=>0.01,'T'=>'buy'}
			elsif ['Kraken', 'Bittrex', 'Gemini', 'Bitstamp'].include?(market_name())
				pair = 'USD-BTC'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>99900.0,'s'=>0.1,'T'=>'sell'}
			elsif ['Polo', 'Binance', 'OKEX', 'HitBTC'].include?(market_name())
				pair = 'BTC-QTUM'
				order_args = {'pair'=>pair,'p'=>0.0001,'s'=>4,'T'=>'buy'}
			elsif market_name() == 'Bybit'
				pair = 'USD-XRP@P'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.8,'v'=>6,'T'=>'buy'}
			elsif market_name() == 'BybitU'
				pair = 'USDT-XRP@P'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.8,'s'=>1,'T'=>'buy'}
			elsif market_name() == 'BybitS'
				pair = 'USDT-BIT'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>1.1,'s'=>10,'T'=>'buy'}
			elsif market_name() == 'BNCM'
				pair = 'USD-BTC@P'
				order_args = {'pair'=>pair,'p'=>20000.0,'v'=>100,'T'=>'buy'}
			elsif market_name() == 'BNUM'
				pair = 'USDT-BTC@P'
				order_args = {'pair'=>pair,'p'=>20000.0,'s'=>0.001,'T'=>'buy'}
			elsif market_name() == 'Bitmex'
				pair = 'BTC-TRX'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.000001,'s'=>2,'T'=>'buy'}
			elsif market_name() == 'Coinbase'
				pair = 'BTC-BCH'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.003,'s'=>0.1,'T'=>'buy'}
			elsif market_name() == 'FTX'
				pair = 'USD-ADA@P'
				pair = determine_pair(pair)
				order_args = {'pair'=>pair,'p'=>0.3, 's'=>10,'T'=>'buy'}
      elsif @_is_ib == true
        # Special test target configuration for IB based markets.
        base_pair = nil
        if market_name() == 'CMECRYPTO'
          base_pair = 'USD-BRR@@0.1'
          order_args = {'pair'=>nil,'p'=>49520,'s'=>nil,'T'=>'buy'}
        elsif market_name() == 'ICECRYPTO'
          base_pair = 'USD-BAKKT@@1'
          order_args = {'pair'=>nil,'p'=>49520,'s'=>nil,'T'=>'buy'}
        elsif market_name() == 'TSE'
          base_pair = 'USD-BTCC.U'
          order_args = {'pair'=>nil,'p'=>8,'s'=>nil,'T'=>'buy'}
        else
          raise "No test_trading_process() task defined for #{market_name()}"
        end
        @_ib_contract_cache_verbose = true
        pairs = match_pairs(base_pair)
        @_ib_contract_cache_verbose = false
        pair = pairs.select { |p|
          next true unless p.include?('@')
          base_pair, expiry, mul = p.split('@')
          (Date.parse(expiry) > Date.today + 7)
        }.sort.first
        puts "Test pair is #{pair}"
        order_args['pair'] = pair
        order_args['s'] = quantity_step(pair)
        puts "Test order args is #{order_args}"
        raise "No such pair from #{pairs}" if pair.nil?
      else
        raise "No test_trading_process() task defined for #{market_name()}"
			end

			if oms_enabled?()
        URN::OMSLocalCache.monitor(
          {market_name() => account_name()},
					[Thread.current], wait:true,
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
    def run_cli(args=ARGV.clone)
      return run_cli_int(ARGV) if args.size > 0
      # Interactive CLI mode if no args given
			if oms_enabled?
				URN::OMSLocalCache.monitor(
					{market_name() => account_name()},
					[Thread.current], wait:true
				)
			end
      loop {
        print "\n#{market_name()} #{account_name()} >"
        input = get_input()
        break(puts("Bye")) if input.nil?
        input = input.strip
        next if input.empty?
        args = input.split(' ')
        begin
          run_cli_int(args)
        rescue => e
          APD::Logger.error e
          puts "Whoops"
        end
      }
    end
    def run_cli_int(args=ARGV.clone)
			@trade_mode = 'ab3'
			@task_name = 'CLI'
			puts "run_cli with args #{args}"
			if args.empty?
				return
			elsif args[0] == 'wsskey'
				# Wait until http pool is created.
				sleep 3 unless @http_persistent_pool.nil?
				# Generate websocket authentication key.
				puts "Websocket key:\n#{wss_key()}"
				return
			elsif args[0] == 'mv'
				loop {
					balance_mv()
					keep_sleep 60
				}
				return
			elsif args[0] == 'keepfull'
				keep_trading_account_full()
				return
			elsif args[0].upcase =~ /^[A-Z]{1,9}TRON$/ # usdttron usdctron
				asset = args[0].upcase.split('TRON')[0]
				test_tron_network(asset)
				return
			elsif args[0] == 'balance'
				balance()
				if args.size > 1
					assets = args[1..-1].map { |a| a.upcase }
					balance_cache_print(assets: assets)
				end
				return
			elsif args[0] == 'balwatch' # Loop querying balance.
				loop {
					balance(verbose:false, silent:true)
					if args.size > 1
						assets = args[1..-1].map { |a| a.upcase }
						balance_cache_print(assets: assets)
					end
					keep_sleep 30
				}
				return
			elsif args[0] == 'margin' || args[0] == 'risk'
				puts JSON.pretty_generate(margin_status(asset: args[1]))
				margin_status_desc(asset: args[1], length: 80).each { |l| puts l }
				return
			elsif args[0] == 'pairs'
				msg = all_pairs().to_a.sort.to_h
				puts JSON.pretty_generate(msg)
				return
			elsif args[0] == 'ban?'
				puts "#{self.class.name}\nbanned?:#{is_banned?()}\nbanned util #{banned_util()}, reason: #{banned_reason()}"
				return
			elsif args[0] == 'ban' && args[1] == 'clear'
				set_banned_util(nil, "N/A")
				return
			elsif args[0] == 'ban' && args.size >= 3
				time = DateTime.parse("#{args[1]}+0800")
				reason = args[2]
				puts "#{self.class.name} set it banned util #{time}, reason: #{reason}"
				set_banned_util(time, reason)
				puts "#{self.class.name} banned util #{banned_util()}, reason: #{banned_reason()}"
				return
			elsif args[0] == 'rate'
				monitor_api_rate()
				return
			elsif args[0] == 'ratemax'
				drop_speed = (args[2] || 1).to_f
				if market_name() == 'Kraken'
					get_input prompt:"Will set #{market_name()} api limit to #{args[1].to_i}, drop_speed #{drop_speed}, press enter"
					api_rate_set_max('manually', new_max: args[1].to_i, drop_speed: drop_speed)
				else
					get_input prompt:"Will set #{market_name()} api limit to original"
					api_rate_set_max('manually')
				end
				return
			elsif args[0] == 't'
				puts 'Fast test()'
				test()
				return
			elsif args[0] == 'test'
				balance()
				results = []
				[args[1].to_i, 1].max.times do |i|
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
			elsif args[0] == 'testbal'
				test_balance_computation()
				return
			elsif args[0] == 'test2'
				loop do
					account_list()
				end
				return
			elsif args[0] == 'test3'
				puts binance_symbol_info('BTC-POLY')
				return
			end

			pair = nil
			filtered_pairs = nil
			asset = args[0].strip.upcase
			if asset[0] != '-' && asset[-1] != '-' && asset.include?('-')
				pair = asset
				asset = pair_assets(pair)[1]
				pair = determine_pair(pair)
      elsif @_is_ib
				pair = "USD-#{asset}"
				pair = determine_pair(pair)
      else
				pair = "BTC-#{asset}"
				full_pairs = all_pairs()
				# If pair is not valid, select a list of pairs for it.
				if full_pairs.include?(pair) == false
					filtered_pairs = full_pairs.keys.select { |p| p.include?(asset) }
					puts "#{filtered_pairs.size} pairs filtered by #{asset}".blue
				else
					pair = determine_pair(pair)
				end
			end
			puts "#{self.class.name} runs in CLI mode, target pair: #{pair||'N/A'}"

			if full_pairs.nil? && args.size > 1 && ['addr', 'fundin', 'fundout', 'borrow', 'repay'].include?(args[1]) == false
				preprocess_deviation_evaluate(pair)
			end
		
      puts "ARGS: #{args.to_json}".blue
			# Commands
			if args[0] == 'his'
				# client.rb his
				orders = history_orders(nil)
				orders.sort_by { |o| o['t'] }.each do |o|
					if o['market'] == 'Bittrex'
						# Print fee rate for bittrex
						rate = ((o['commission']||o['Commission']||o['CommissionPaid']) / o['p'] / o['executed'] * 100.0).round(3)
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']} \t#{rate}%\n"
					else
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']}\n"
					end
				end
				puts "Totally #{orders.size}"
				return
			elsif pair != nil && args[1] == 'his'
				# client.rb usdt-btc his
				orders = history_orders(pair)
				orders.sort_by { |o| o['t'] }.each do |o|
					if market_name == 'Bittrex'
						# Print fee rate for bittrex
						rate = (o['commission'] /o['p']/o['executed'] * 100.0).round(4)
						print "#{format_trade(o)}\n#{o['pair']} \t#{o['i']} \t#{rate}%\n"
					else
						print "#{format_trade(o)}\n#{o['i']}\n"
					end
				end
				puts "Totally #{orders.size}"
				return
			elsif args[0] == 'alive' # client.rb alive
				active_orders(nil)
				return
			elsif args[0] == 'funding'
				his = funding_fee_history(nil)
				his.sort_by { |r| DateTime.parse(r['time_str']) }.each { |r|
					# puts JSON.pretty_generate(r)
					print [
						r['time_str'], r['pair'].ljust(12), r['asset'].ljust(5),
						format_num(r['amount'], 8, 4), "\n"
					].join(' ')
				}
				return
			elsif pair != nil && args[1] == 'alive'
				# client.rb usdt-btc alive
				active_orders(pair)
				return
			elsif pair != nil && ['borrow', 'repay', 'fundin', 'fundout', 'limit'].include?(args[1])
				# client.rb eth fundin 100
				# client.rb eth fundout 100
				# client.rb eth borrow 100
				# client.rb eth repay 100
				# client.rb eth repay all
				balance()
				type, amount = args[1], args[2]
				amount = amount.to_f unless type == 'repay' && amount == 'all'
				if type == 'limit'
					puts "Max borrowable of #{asset} is #{margin_max_borrowable(asset)}".blue
				elsif type == 'fundin'
					type = 'in'
					fund_in_out(asset, amount, type, allow_fail:true)
				elsif type == 'fundout'
					type = 'out'
					puts "#{type} #{asset} #{amount}".red
					ret = get_input prompt:"Enter YES to #{type} #{asset} #{amount}".red
					exit if ret != "YES"
					fund_in_out(asset, amount, type, allow_fail:true)
				elsif type == 'borrow'
					# Show max borrowable amount.
					puts "Max borrowable amount: #{margin_max_borrowable(asset)}"
					puts "#{type} #{asset} #{amount}".red
					if args[2].downcase == 'all'
						amount = margin_max_borrowable(asset, opt={})
						raise "Failed to get margin_max_borrowable(#{asset})" if amount.nil?
					end
					ret = get_input prompt:"Enter YES to #{type} #{asset} #{amount}".red
					exit if ret != "YES"
					borrow_repay(asset, amount, type, allow_fail:true)
				elsif type == 'repay'
					borrow_repay(asset, amount, type, allow_fail:true)
				end
				return
			elsif pair != nil && args[1] == 'rule'
				# client.rb usdt-btc rule
				balance() if market_type() == :future
				pairs = ['BTC', 'ETH', 'USDT', 'USD', 'TRX'].map { |base| "#{base}-#{asset}" }
        pairs = ["USD-#{asset}"] if @_is_ib
				pairs = [pair] if market_type() == :future
				pairs.each do |pair|
					puts ['#'*20, 'Rules for', pair, '#'*20].join(' ')
          if @_is_ib
            match_pairs(pair)
          else
            if all_pairs().include?(pair) == false
              puts "Not supported".red
              next
            end
          end
					preprocess_deviation_evaluate(pair)
					puts [pair, "min_vol", min_vol(pair)]
					base = pair_assets(pair)[0]
					if base == 'BTC' && args[2] != nil
						puts [pair, "min_order_size", min_order_size(pair, args[2].to_f)]
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
					if market_type() == :furure
						puts [pair, 'future_position', future_position(pair)]
						puts [pair, 'future_position_cost', future_position_cost(pair)]
						puts [pair, 'future_max_position_cost(buy) ', future_max_position_cost(pair, 'buy')]
						puts [pair, 'future_max_position_cost(sell)', future_max_position_cost(pair, 'sell')]
					end
				end
				return
			elsif pair != nil && args[1] == 'bal'
				# client.rb usdt-btc bal
				test_balance_with_all_orders(pair)
				return
			elsif pair != nil && args[1] == 'fee'
				ftx_enter_spot_mode() if market_name() == 'FTX'
				# client.rb usdt-btc fee
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
					puts "high_withdraw_fee_deviation [#{asset}]: #{high_withdraw_fee_deviation(asset)}"
				else
					puts "Withdraw fee [#{asset}]: NON transferrable.".red
				end
				return
			elsif pair != nil && args[1] == 'funding'
				hours = (args[2] || '72').to_i
				his = funding_fee_history(pair)
				puts JSON.pretty_generate(his)
				his.sort_by { |r| DateTime.parse(r['time_str']) }.each { |r|
					# puts JSON.pretty_generate(r)
					print [
						r['time_str'], r['pair'].ljust(12), r['asset'].ljust(5),
						format_num(r['amount'], 8, 4), "\n"
					].join(' ')
				}
				premium = future_premium(pair, allow_fail:true)
				puts JSON.pretty_generate(premium.dig(pair))
				puts chart_string_for_funding_rate_history(pair, hours)
				return
			elsif pair != nil && args[1] == 'funding_stat'
				pair = pair.upcase
				if is_future?(pair)
					;
				else
					pair = all_pairs().keys.
						select { |p| is_future?(p) && p.split('@')[0].split('-')[1] == pair }.first
					raise "No pair matches #{args[1]}" if pair.nil?
				end
				hours = (args[2] || 48).to_i
				funding_stat = funding_rate_stat(pair, hours)
				premium = future_premium(pair, allow_fail:true)
				puts JSON.pretty_generate(funding_stat)
				puts JSON.pretty_generate(premium.dig(pair))
				puts chart_string_for_funding_rate_history(pair, hours)
				puts "#{hours} hours"
				return
			elsif pair != nil && args[1] == 'addr'
				# client.rb btc addr
				puts "#{self.class.name} deposit [#{asset}]:\n#{deposit_addr(asset)}"
				return
			elsif pair != nil && args[1] == 'tx'
				# client.rb btc tx
				transactions(asset, limit:99, watch:false).sort_by { |tx| tx['t'] }.each { |tx|
					print "#{format_transaction_log(tx)}\n"
				}
				return
			elsif pair != nil && args[1] == 'premium'
				# client.rb usdt-btc market
				if self.respond_to?(:future_premium)
					puts JSON.pretty_generate(future_premium(pair)[pair])
				else
					puts "Not implemented."
				end
				return
			elsif pair != nil && args[1] == 'market'
				# client.rb usdt-btc market
				if self.respond_to?(:market_summary)
					summary = market_summary(pair)
					puts JSON.pretty_generate(summary)
				else
					puts "Not implemented."
				end
				return
      elsif pair != nil && args.size == 2 && args[1].to_s =~ /^[_\-0-9A-Za-z\.]{5,64}$/
				# client.rb usdt-btc 12345678
				# client.rb usdt-btc CLIENTOID12345678
				order = {'market'=>market_name(), 'pair'=>pair, 'i'=>args[1]}
        puts "querying #{order}"
				if args[1].start_with?('CLIENTOID')
					client_oid = args[1]['CLIENTOID'.size..-1]
					puts "client_oid mode"
					order = {'market'=>market_name(), 'pair'=>pair, 'client_oid'=>client_oid}
				else
					puts "order id mode #{args[1]}"
				end
				loop do
					o = query_order(pair, order)
					puts JSON.pretty_generate(o)
					puts format_trade(o)
					break unless order_alive?(o)
					sleep 1
				end
				# Also query_order in api V3 for Bittrex.
				query_order(order['pair'], order)
				return
			end

			# Market action
			if oms_enabled?()
				URN::OMSLocalCache.monitor({market_name()=>account_name()}, [Thread.current], wait:true)
			end
			balance() if @balance_cache.nil?
			if pair != nil && args[1] == 'cancel' && args[2] != nil
				# client.rb usdt-btc cancel 12345678
				# client.rb usdt-btc cancel all
				# client.rb usdt-btc cancel all sell
				# client.rb usdt cancel 12345678
				id = args[2].strip
				if filtered_pairs != nil
					puts "Cancelling #{filtered_pairs.join(',')} #{id}"
					orders = active_orders(nil).select { |o|
						filtered_pairs.include?(o['pair'])
					}
					puts "Active orders: #{filtered_pairs.join(',')} #{orders.size} found"
					orders.each { |o|
						puts "#{o['pair'].ljust(10)} #{o['i']}\n#{format_trade(o)}"
					}
				else
					puts "Cancelling #{pair} #{id}"
					orders = active_orders(pair)
				end
				if orders.empty?
					puts "No alive orders"
				elsif id == 'all'
					target_orders = orders.select do |t|
						next false unless order_alive?(t)
						# skip small orders.
						if market_type == :spot && full_pairs.nil?
							if t['p']*t['remained'] < min_vol(pair)
								puts "skip tiny spot order #{t['i']}:\n#{format_trade(t)}"
								next false
							end
						end
						if args[3].nil?
							next true
						elsif args[3] == 'buy'
							next (t['T'] == 'buy')
						elsif args[3] == 'sell'
							next (t['T'] == 'sell')
						else
							next false
						end
						puts "will cancel #{t['i']}:\n#{format_trade(t)}"
					end
					input = get_input prompt:"Press Y to cancel all [#{args[3]}] orders"
					raise "Abort canceling order" unless input.downcase.strip == 'y'
					canceled_orders = []
					loop do
						cancel_orders = cancel_orders(nil, target_orders, allow_fail:true)
						break if cancel_orders != nil && canceled_orders.select{ |o| order_alive?(o) }.empty?
						sleep 3
					end
					canceled_orders.each do |o|
						puts "Canceled #{t['i']}:\n#{format_trade(t)}"
					end
				else
					timeout = (args[3] || '10').to_i
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
			elsif pair != nil && args.size >= 4 && ['buy', 'sell'].include?(args[1])
				preprocess_deviation_evaluate(pair)
				# client.rb usdt-btc buy 10000.0 0.1
				# Placing single new order manually.
				order_args = {
					'pair' => pair,
					'T'	=> args[1],
					'p'	=> args[2].to_f
				}
				amount = args[3].to_f
				if args[3].downcase == 'all'
					bal = max_order_size(pair,	order_args['T'],	order_args['p'], clear_bal: true)
					get_input prompt:"Use balance to determine size to #{bal}, enter to continue"
					amount = bal
				end
				order_args['s'] = amount
				order_args['v'] = order_args.delete('s') if quantity_in_orderbook() == :vol
				tif = nil
				tif = 'PO' if args.include?('po')
				loop do
					input = get_input prompt:"Press Y to confirm #{pair} #{tif} order\n#{format_trade(order_args)}"
					raise "Abort placing order" unless input.downcase.strip == 'y'
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
			elsif pair != nil && args.size >= 4 &&
				['step'].include?(args[1]) &&
				['buy', 'sell'].include?(args[2])
				# step buy/sell start_price end_price step size
				start_price, end_price, step_price, size = args[3..6].map { |s| s.to_f }
				orders = []
				vol, size_ttl = 0, 0
				step_price = 0-step_price if end_price < start_price
				((end_price-start_price).abs/step_price).ceil.times do |i|
					order = {
						'T'	=> args[2],
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
				raise "Abort placing order" unless input.downcase.strip == 'y'
				orders.each do |o|
					o = place_order pair, o
					puts "Order placed #{order['i']}:\n#{format_trade(o)}"
				end
			end

			preprocess_deviation_evaluate(pair) if full_pairs.nil?
			# List orders at last by default.
			if filtered_pairs != nil
				puts "Active orders: #{filtered_pairs.join(',')}"
				orders= active_orders(nil).select { |o|
					filtered_pairs.include?(o['pair'])
				}
				puts "Active orders: #{filtered_pairs.join(',')} #{orders.size} found"
			else
				puts "Active orders: #{pair}"
				orders = active_orders(pair)
			end
			orders.each { |o| puts "#{o['pair'].ljust(10)} #{o['i']}\n#{format_trade(o)}" }
		end
	end

	# For querying trading pairs in multi-market clients
	module MarketPairUtil

		# Convert pair to spider broadcast pair.
		def local_pair(pair, client)
			return client.bfx_shrink_pair(pair) if client.is_a?(URN::BFX)
			return client.pair_to_underlying_pair(pair) if client.respond_to?(:pair_to_underlying_pair)
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
				puts "choose_best_market for #{mkt} max_optimize_gap #{max_optimize_gap}" if debug
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
				mkt_stat_map[mkt] = {:bal => balance.round(8), :score => score}
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
			size = orders.map { |o| # Cap order size with both balance again !
				if o['T'] == 'buy'
					o['s'] = [o['s'], mkt_client_ask.max_order_size(o)].min
				else
					o['s'] = [o['s'], mkt_client_bid.max_order_size(o)].min
				end
				o['s']
			}.min
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
			@_mkt_data_valid_warning ||= {}
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
					@_mkt_data_valid_warning[m] ||= 0
					if now - @_mkt_data_valid_warning[m] > 10*1000
						puts "#{m} #{@pair} orderbook aborted, #{abort_reason}".red
						@_mkt_data_valid_warning[m] = now
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

			@_mkt_data_valid_warning ||= {}
			@_mkt_data_slow_warning ||= {}
			data_chg = false
			now = (Time.now.to_f*1000).to_i
			odbk_list = opt[:data] || latest_orderbook(mkt_clients, pair_list, opt)
			mkt_clients.zip(odbk_list, pair_list).each do |client, odbk, pair|
				next if odbk.nil?
				# Add real price with commission to each order
				_preprocess_orderbook(pair, odbk, client) if opt[:no_real_p] != true
				snapshot[client.given_name] ||= {}
				snapshot[client.given_name][:orderbook] = odbk
				# Also compatible with mp keys
				snapshot[client.given_name][pair] ||= {}
				snapshot[client.given_name][pair][:orderbook] = odbk
				bids, asks, t, mkt_t = odbk
				t = mkt_t if mkt_t != nil
				# Abort if timestamp is too old compared to system timestamp.
				abort_reason = nil
				gap = now - t.to_i
				abort_reason = "#{gap/1000} seconds ago" if gap > 60*1000
				m = client.market_name()
				if abort_reason != nil
					@_mkt_data_valid_warning[m] ||= 0
					if now - @_mkt_data_valid_warning[m] > 600*1000
						puts "#{m} #{pair} odbk -X-> data_chg, #{abort_reason}".red
						@_mkt_data_valid_warning[m] = now
					end
					next
				end
				# Check market timestamp with latest market_client timestamp.
				time_legacy = client.last_operation_time.strftime('%Q').to_i - t
				if time_legacy >= 5
					@_mkt_data_slow_warning[m] ||= 0
					if now - @_mkt_data_slow_warning[m] >= 200
						@_mkt_data_slow_warning[m] = now
						puts "#{m} #{pair} orderbook is #{time_legacy.round}ms old"
					end
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
				t = mkt_t if mkt_t != nil
	
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
				# Also compatible with mp keys
				snapshot[market][pair] ||= {}
				snapshot[market][pair][:trades] = odbk
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
          if trade['t'].is_a?(Integer)
            trade['t'] = trade['t'].to_s
          elsif trade['t'].to_i.to_s != trade['t']
            begin
              trade['t'] = DateTime.parse("#{trade['t']}+0800}").strftime('%Q')
            rescue
              puts "Invalid trade date #{trade}".red
            end
          end
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
		raise "Input #{test_file} is not found" unless File.file? test_file
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
		raise "Input #{test_file} is not found" unless File.file? test_file
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
		raise "Test3-1 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.0000119, adjust:true, num:true) != 0.000011
		raise "Test3-1 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.0000119, adjust:true) != '0.000011'
		raise "Test3-2 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.0000119, adjust:true, num:true) != 0.000012
		raise "Test3-2 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.0000119, adjust:true) != '0.000012'
		raise "Test3-3 failed" if hitbtc.format_price_str('BTC-DNT', 'buy', 0.000011) != '0.000011'
		raise "Test3-4 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 0.000011) != '0.000011'
		raise "Test3-5 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 11) != '11'
		raise "Test3-5 failed" if hitbtc.format_price_str('BTC-DNT', 'sell', 11) != '11'
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
		URN::OMSLocalCache.monitor(URN::TRADE_MARKETS, [Thread.current], wait:true)
		# Run all test for all trade market.
		URN::TRADE_MARKETS.each do |m|
			client = client_register URN.const_get(m).new(verbose:true, trade_mode:'test')
			puts "Running test_trading_process() for #{m}".blue
			client.balance()
			client.test_trading_process()
		end
		URN::TRADE_MARKETS.each do |m|
			puts "√ test_trading_process() passed #{m}".green
		end
	elsif ARGV[0] == 'proxy_benchmark'
		URN::OMSLocalCache.monitor(URN::TRADE_MARKETS, [Thread.current], wait:true, verbose:false)
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
	elsif ARGV[0] == 'ban?'
		URN::TRADE_MARKETS.map { |m|
			c = URN.const_get(m).new(verbose:true, trade_mode:'no', task_name:'cli')
			next nil if c.banned_reason().nil?
			next "#{c.market_name()} banned?:#{c.is_banned?()}\nbanned util #{c.banned_util().to_s.red}, reason: #{c.banned_reason()}\n"
		}.each { |s|
			next if s.nil?
			puts s
		}
	else
		puts "Unknown args [#{ARGV}]"
	end
end
