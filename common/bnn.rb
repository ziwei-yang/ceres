require_relative '../common/bootstrap'

# Binance client api.
# Supports allow_fail in trade functions.
module URN
	class BinanceError < RuntimeError
	end
	module BinanceAPIFramework
		include APD::LockUtil
		include APD::CLI

		######### Make request #########
		# The /api/v3/exchangeInfo rateLimits array contains objects 
		# related to the exchange's RAW_REQUEST, REQUEST_WEIGHT, and ORDER rate limits.
		# 2021-04-23 currently: 1200 weight per 1 minute, but seems 700 is the cap.
		def api_rate_rule
			binance_symbol_info() if @binance_symbol_info.nil?
			# Init from /api/v3/exchangeInfo binance_symbol_info()
			data = {
				'rule' => {
					'weight' => [@binance_weight_limits['limit'], @binance_weight_limits['second']],
					'order' => [@binance_order_limits['limit'], @binance_order_limits['second']]
				},
				'score' => {
					'weight' => @binance_weight_limits['limit'],
					'order' => @binance_order_limits['limit'],
				},
				'his' => [],
				'extra' => []
			}
		end

		######### Make request #########
		def binance_req(path, opt={})
			original_path = path
			loop do
				path = original_path
				timeout = 10
				timeout = 60 if opt[:place_order] == true
				return nil if opt[:allow_fail] == true && is_banned?()
				wait_if_banned() unless opt[:wss_key] == true
	
				account = opt[:account]
				method = opt[:method] || :GET
				args = {}
				(opt[:args] || {}).each { |k, v| args[k] = v }
				display_args_str = args.to_a.
					sort_by { |kv| kv[0] }.
					map { |kv| kv[0].to_s + '=' + kv[1].to_s }.join('&')

				weight = opt[:weight] || 0
				emergency_call = (opt[:cancel_order] == true || opt[:emergency_call] == true)
				memo = "#{path} #{opt[:memo] || display_args_str} #{emergency_call ? "EMG".red : ""}"
				loop {
					break if opt[:skip_api_rate_control] == true
					break if opt[:wss_key] == true
					break if @binance_rate_control == false
					should_call = api_rate_control(weight, emergency_call, memo, opt)
					break if should_call
					if should_call == false && opt[:allow_fail] == true
						puts "Abort request because of rate control"
						return nil
					end
					puts "Should not call api right now, wait: #{path} #{display_args_str}"
					keep_sleep 1
				}

				args[:timestamp] = (Time.now.to_f*1000).to_i
				args[:recvWindow] = 60*1000 # more time tolerance.
	
				args_str = args.to_a.
					sort_by { |kv| kv[0] }.
					map { |kv| kv[0].to_s + '=' + kv[1].to_s }.join('&')
				args[:signature] = signature = binance_sign(args_str)
				args_str = "#{args_str}&signature=#{signature}"
				header = { :'X-MBX-APIKEY' => @BINANCE_API_KEY }
				if opt[:public] == true
					args_str = display_args_str
					header = {}
				end
	
				domain = @BINANCE_API_DOMAIN
				force_lib = nil
				if path.include?('/v3/')
					;
				elsif path.include?('/fapi/') || path.include?('/dapi/')
					force_lib = :restclient if method == :POST # BNCM BNUM with http_pool does not support POST
				elsif path.include?('/sapi/')
					force_lib = :restclient # All those http_pool staff does not work with /sapi/
				else
					raise "Unexpected path"
				end

				begin
					payload = nil
					if method == :GET
						url = "#{domain}#{path}?#{args_str}"
					elsif method == :POST
						url = "#{domain}#{path}"
						payload = args_str
					elsif method == :WSSKEY
						url = "#{domain}#{path}"
						payload = ''
						method = :POST
					elsif method == :WITHDRAW
						url = "#{domain}#{path}?#{args_str}"
						payload = ''
						method = :POST
					elsif method == :DELETE
						url = "#{domain}#{path}?#{args_str}"
					else
						raise "Unknown http method:#{method}"
					end

					response, proxy = mkt_http_req(
						method, url,
						header: header, # mkt_http_req ignores headers when using http pool
						timeout: timeout, # mkt_http_req ignores timeout when using http pool
						force_lib: force_lib,
						payload: payload, display_args: "#{path} #{display_args_str}",
						silent: opt[:silent]
					)

					begin
						response = JSON.parse(response)
						if response.is_a?(Hash)
							if response['code'].is_a?(Integer) && response['code'] < 0
								# http_pool error would not raise a http error.
								raise BinanceError.new(response.to_json)
							elsif response['success'].nil? && response['msg'] != nil &&
									response['msg'] =~ /^Unknown error/
								# Unknown error, please check your request or try again later.
								puts response.to_s.red
								return nil if opt[:allow_fail] == true
								raise OrderMightBePlaced.new if opt[:place_order] == true
								puts "Try again after 3s"
								keep_sleep 3
								next
							elsif response['success'] == false &&
									response['msg'] == 'System abnormality'
								now = DateTime.now
								puts "System abnormality, maybe exchange is under maintenance."
								t = banned_util()
								if t.nil? || t < (now + 30.0/86400.0)
									# Wait 30-90 seconds
									t = (now + (30.0+Random.rand(90))/86400.0)
									set_banned_util(t, response.to_json)
								end
							end
						end
					rescue JSON::ParserError => e
						now = DateTime.now
						puts ['JSON parsing error', e.message]
						if e.message.include?('502 ERROR') && 
								e.message.include?('The request could not be satisfied') &&
								e.message.include?('Generated by cloudfront')
							# Gateway error, postpone banned timer.
							t = banned_util()
							if t.nil? || t < (now + 30.0/86400.0)
								# Wait 30-90 seconds
								t = (now + (30.0+Random.rand(90))/86400.0)
								set_banned_util(t, response.to_json)
							end
						end
						return nil if opt[:allow_fail] == true
						raise OrderMightBePlaced.new if opt[:place_order] == true
						puts "Try again after 3s"
						keep_sleep 3
						next
					end
					return response
				rescue Zlib::BufError, SOCKSError, RestClient::Exception, Net::HTTPBadResponse, Net::HTTPFatalError, HTTP::Error, BinanceError => e
					puts "proxy:#{proxy}"
					err_msg, err_res = '', ''
					if e.is_a?(BinanceError) # BinanceError only occurred in http_pool mode.
						puts ['API failed', e.message]
						err_msg, err_res = "", e.message.to_s
					elsif e.is_a?(RestClient::Exception)
						puts ['API failed', e.message, e.response.to_s]
						err_msg, err_res = e.message.to_s, e.response.to_s
					else
						puts ['API failed', e.class, e.message]
						err_msg = e.message.to_s
					end
					now = DateTime.now
					# https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
					# Check any IP banned info.
					if err_msg.start_with?('418 ')
						# Repeatedly violating rate limits and/or failing to back off 
						# after receiving 429s will result in an automated IP ban (http status 418).
						if err_res.include?('"code":-1003,') && err_res.include?('IP banned until')
							banned_util_s = err_res.split('IP banned until')[1].split('.')[0].strip
							raise "Unexpected banned info #{banned_util_s.inspect}" unless banned_util_s =~ /^[0-9]*$/
							banned_util_t = DateTime.strptime banned_util_s, '%Q'
							set_banned_util(banned_util_t, "#{err_msg} #{err_res}")
							return nil if opt[:allow_fail] == true
							next
						elsif err_res.include?('"code":-1003,') && err_res.include?('Too much request weight used')
							t = banned_util()
							if t.nil? || t < (now + 1.0/1440.0)
								# Wait 1-2 min
								t = (now + (10.0+Random.rand(10))/14400.0)
								set_banned_util(t, "#{err_msg} #{err_res}")
							end
							return nil if opt[:allow_fail] == true
							next
						else
							raise "Unexpected 418 banned info."
						end
					elsif err_msg.start_with?('429 ') || (err_msg.start_with?('403 ') && err_res.include?("The request could not be satisfied"))
						# When a 429 is received, it's your obligation as an API to back off 
						# and not spam the API.
						# When 403 received, maybe marked as spam by cloudflare.
						puts "warning, postpone banned_util to avoid IP to be banned"
						t = banned_util()
						if t.nil? || t < (now + 3.0/1440.0)
							# Wait 2-10 min
							t = (now + (20.0+Random.rand(80))/14400.0)
							set_banned_util(t, "#{e.message} #{e.response.to_s}")
						end
						return nil if opt[:allow_fail] == true
						next
					end
					# BinanceError would be raised if http_pool is applied.
					# Otherwise, Binance raises Http 400 error if args is unaccpeted.
					if e.is_a?(BinanceError) || err_msg.include?('400 Bad Request') || err_msg.include?('503 Service Unavailable')
						if err_res.include?('"code":-1001,')
							if err_res.include?('Internal error')
								return nil if opt[:allow_fail] == true
								next
							else
								raise "Unexpected -1001 message."
							end
						elsif err_res.include?('"code":-1003,')
							if err_res.include?('IP banned until')
								banned_util_s = err_res.split('IP banned until')[1].split('.')[0].strip
								raise "Unexpected banned info #{banned_util_s.inspect}" unless banned_util_s =~ /^[0-9]*$/
								banned_util_t = DateTime.strptime banned_util_s, '%Q'
								set_banned_util(banned_util_t, "#{err_msg} #{err_res}")
								return nil if opt[:allow_fail] == true
								next
							elsif err_res.include?('Too much request weight used')
								t = banned_util()
								if t.nil? || t < (now + 1.0/1440.0)
									# Wait 1-2 min
									t = (now + (10.0+Random.rand(10))/14400.0)
									set_banned_util(t, "#{err_msg} #{err_res}")
								end
								return nil if opt[:allow_fail] == true
								next
							else
								raise "Unexpected -1003 message."
							end
						elsif err_res.include?('"code":1,') && err_res.include?('System is under maintenance')
							return nil if opt[:allow_fail] == true
							keep_sleep 120
							next
						elsif (err_res.include?('"code":-1010,') || err_res.include?('"code":-1013,')) && err_res.include?('Market is closed')
							return nil if opt[:allow_fail] == true
							keep_sleep 120
							next
						elsif err_res.include?('"code":-1013,') && err_res.include?('Filter failure: PERCENT_PRICE')
							# Price is too high/low to create order.
							return nil if opt[:allow_fail] == true
							raise OrderArgumentError.new(err_res)
						elsif err_res.include?('"code":-1013,') && err_res.include?('Filter failure')
							raise OrderArgumentError.new(err_res)
						elsif err_res.include?('"code":-1015,') && err_res.include?('Too many new orders')
							return nil if opt[:allow_fail] == true # current limit is 100 orders per 10 SECOND
							keep_sleep(20+Random.rand(20))
							next
						elsif err_res.include?('"code":-1021,')
							puts "Timestamp is too early, caused by network lag, request again."
							next
						elsif err_res.include?('"code":-1100,')
							puts "Illegal characters found in a parameter, request again."
							return nil if opt[:allow_fail] == true # current limit is 100 orders per 10 SECOND
							keep_sleep 1
							next
						elsif err_res.include?('"code":-1121,')
							puts "Invalid symbol"
							raise TradingPairNotExist.new(err_res)
						elsif err_res.include?('"code":-2010,')
							puts ["NEW_ORDER_REJECTED", err_res]
							if err_res.include?('MAX_NUM_ORDERS')
								puts "Severe BUG appearred, too many alive orders"
								raise err_res
							elsif err_res.include?('Duplicate order sent')
								raise OrderAlreadyPlaced.new(err_res)
							elsif err_res.include?('Account has insufficient balance for requested action')
								raise NotEnoughBalance.new(err_res)
							elsif err_res.include?('Rest API trading is not enabled')
								t = banned_util()
								if t.nil? || t < (now + 30.0/86400.0)
									# Wait 30-90 seconds
									t = (now + (30.0+Random.rand(90))/86400.0)
									set_banned_util(t, "#{err_msg} #{err_res}")
								end
							elsif err_res.include?('Order would immediately match and take')
								# Should adjust price again.
								return nil if opt[:allow_fail] == true
								raise OrderArgumentError.new(err_res)
							else
								puts "This is an untreat reason that stop placing order"
							end
							return nil if opt[:allow_fail] == true
							next
						elsif err_res.include?('"code":-2011,')
							if err_res.include?('Rest API trading is not enabled')
								t = banned_util()
								if t.nil? || t < (now + 30.0/86400.0)
									# Wait 30-90 seconds
									t = (now + (30.0+Random.rand(90))/86400.0)
									set_banned_util(t, "#{err_msg} #{err_res}")
								end
							elsif err_res.include?('Unknown order')
								puts "Order is unknown."
								raise OrderNotExist.new(err_res)
							else
								puts "Unknown error"
								raise e
							end
							return nil if opt[:allow_fail] == true
							next
						elsif err_res.include?('"code":-2013,')
							puts "Order does not exist."
							raise OrderNotExist.new(err_res)
						elsif err_res.include?('"code":-2022,')
							puts "ReduceOnly Order is rejected."
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('"code":-3020,')
							puts "Balance is not enough"
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('"code":-3041,')
							puts "Balance is not enough"
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('"code":-3045,') # Borrow
							puts "The system does not have enough asset now"
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('"code":-4093,')
							puts "The deposit has been closed"
							raise ActionDisabled.new(err_res)
						elsif err_res.include?('"code":-6006,')
							puts "Redeem amount error."
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('"code":-11001,')
							puts "Isolated margin account does not exist."
							raise ActionDisabled.new(err_res)
						elsif err_res.include?('"code":-11015,')
							puts "Balance is not enough"
							raise NotEnoughBalance.new(err_res)
						elsif err_res.include?('Unknown error, please check your request or try again later.')
							raise OrderMightBePlaced.new if opt[:place_order] == true
							return nil if opt[:allow_fail] == true # No code in this case
							keep_sleep 1
							next
						end
						raise e
					end
					if err_msg.include?('401 Unauthorized')
						if err_res.include?('"code":-2015,')
							puts "Invalid API-key, IP, or permissions for action."
							t = banned_util()
							if t.nil? || t < (now + 30.0/86400.0)
								# Wait 30-90 seconds
								t = (now + (30.0+Random.rand(90))/86400.0)
								set_banned_util(t, "#{err_msg} #{err_res}")
							end
							return nil if opt[:allow_fail] == true
							next
						end
						raise e
					end
					# For other unexpected errors, assume order might be placed.
					raise OrderMightBePlaced.new if opt[:place_order] == true
					return nil if opt[:allow_fail] == true
					keep_sleep 1
					next if err_msg.include?('301 Moved Permanently')
					next if normal_api_error?(e)
					raise e
				rescue ThreadError => e
					if opt[:allow_fail] == true
						puts "Thread error #{e.message}"
						return nil
					end
					keep_sleep 1
					next if (e.message || '').include?('Resource temporarily unavailable')
					raise e
				rescue OpenSSL::SSL::SSLError, Errno::ECONNREFUSED, SocketError, Errno::EHOSTUNREACH, Errno::ETIMEDOUT, Errno::ENETUNREACH, Errno::ECONNRESET, Errno::EPIPE => e
					puts e.message
					next
				end
			end
		end
		def binance_sign(data, opt={})
			OpenSSL::HMAC.hexdigest(@sha256_digest, @BINANCE_API_SEC, data)
		end
	end

	class Binance < TradeClientBase; end
	TradeClient['Binance'] = Binance
	class Binance
		include APD::ExpireResult
		include URN::BinanceAPIFramework
		def oms_enabled?
			true
		end

		def initialize(opt={})
			@could_be_banned = true
			@BINANCE_API_DOMAIN = ENV['BINANCE_API_DOMAIN_V3'] || raise('BINANCE_API_DOMAIN_V3 is not set in ENV')
			@BINANCE_API_KEY ||= ENV['BINANCE_API_KEY_WITHDRAW'] || raise('BINANCE_API_KEY_WITHDRAW is not set in ENV')
			@BINANCE_API_SEC ||= ENV['BINANCE_API_SEC_WITHDRAW'] || raise('BINANCE_API_SEC_WITHDRAW is not set in ENV')
			@http_proxy_str = @BINANCE_PROXY = (ENV['BINANCE_API_PROXY'] || 'default').
				split(',').
				map { |str| str=='default'?nil:str }
			# Enable http_pool for non-affliate binance account only.
			# http_pool API respond time: 60+ ms
			# restclient API respond time: 80+ ms
			if self.class.name.split('::').last == 'Binance'
				opt = opt.merge(
					http_pool_host: @BINANCE_API_DOMAIN,
					http_pool_headers: { :'X-MBX-APIKEY' => @BINANCE_API_KEY },
					http_pool_keepalive_timeout: 240,
					http_pool_op_timeout: { read: 10, write: 5, connection: 2 }
				)
				# puts opt.inspect
				@binance_rate_control = true
			else
				@binance_rate_control = false
			end
			super(opt)
		end

		def is_transferable?
			true
		end

		def deposit_fee(asset, opt={})
			0
		end

		def can_deposit?(asset, opt={})
			asset = asset.upcase
			return false if asset == 'SALT'
			binance_asset_info()
			if opt[:network].nil?
				return @can_deposit_map[asset] == true
			elsif opt[:network] == 'TRON'
				return @can_deposit_by_network.dig(asset, 'TRX') == true
			else
				return @can_deposit_by_network.dig(asset, opt[:network]) == true
			end
		end

		def can_withdraw?(asset, opt={})
			asset = asset.upcase
			return false if asset == 'SALT'
			binance_asset_info()
			if opt[:network].nil?
				return @can_withdraw_map[asset] == true
			elsif opt[:network] == 'TRON'
				return @can_withdraw_by_network.dig(asset, 'TRX') == true
			else
				return @can_withdraw_by_network.dig(asset, opt[:network]) == true
			end
		end

		def support_tron?(asset='USDT')
			deposit_addr(asset, network: 'TRON') != nil
		end

		def withdraw_fee(asset, opt={})
			binance_asset_info()
			if opt[:network].nil?
				return @binance_fee_map[asset] || 99999
			elsif opt[:network] == 'TRON'
				return @binance_fee_by_network.dig(asset, 'TRX') || 99999
			else
				return @binance_fee_by_network.dig(asset, opt[:network]) || 99999
			end
		end

		def off_withdraw_fee_deviation(asset)
			# Return zero when account has borrowed in margin account.
			margin_balance(silent:true, allow_fail:true)
			return 0.2 if @margin_balance.nil?
			borrowed = @margin_balance.dig(asset, 'borrowed')
			if borrowed.nil? || borrowed == 0
				return 0.2 # 0.05 is not enough for evil exchanges
			else
				return 0 # While has some debt, it is okay to buy back even asset can not be withdraw.
			end
		end

		HIGH_FEE_ASSETS = []
		def high_withdraw_fee_deviation(asset)
			asset = asset.upcase
			# Check withdraw_fee for USDT to determine ETH token series fee.
			if asset == 'USDT' # Use TRON network for USDT
				return nil
			elsif asset == 'ETH' # Huge buffer for ETH
				return nil
			elsif URN::USD_TOKENS.include?(asset)
				usdt_fee = withdraw_fee('USDT')
				if usdt_fee > 20
					return (usdt_fee.to_f/20000).ceil(3)
				end
				return nil
			elsif URN::ETH_TOKENS.include?(asset)
				# Also depends on volume of USDT-ASSET / BTC-ASSET ?
				usdt_fee = withdraw_fee('USDT')
				r = nil
				if usdt_fee > 20
					r = (usdt_fee.to_f/20000).ceil(3)
				end
				# Some typical high fee assets.
				if ['AAVE', 'DNT', 'CVC', 'BAT', 'COMP', 'STORJ', 'ZRX'].include?(asset)
					if r.nil?
						r = 1/250.0
					else
						r *= 2
					end
				end
				return r
				# ETH withdraw_fee should mul to its price, use USDT fee instead.
			end
			return 0.005 if HIGH_FEE_ASSETS.include?(asset)
			nil
		end

		def all_pairs(opt={})
			binance_symbol_info('BTC-ETH')
			res = @binance_symbol_info.values.map do |r|
				r['baseAsset'] = 'BCH' if r['baseAsset'] == 'BCC'
				pair = "#{r['quoteAsset']}-#{r['baseAsset']}"
				pair = underlying_pair_to_pair(pair)
				symbol = r['symbol']
				[pair, symbol]
			end.to_h
			res.delete('USD-XEM') # Delisted but not in API.
			res
		end

		def binance_asset_info()
			@binance_fee_map ||= {}
			@can_deposit_map ||= {}
			@can_withdraw_map ||= {}

			@binance_fee_by_network ||= {}
			@can_deposit_by_network ||= {}
			@can_withdraw_by_network ||= {}
			res = nil
			begin
				res = redis_cached_call("V1CapitalConfigGetall", 600) {
					binance_req '/sapi/v1/capital/config/getall', weight: 1, method: :GET, allow_fail:true, emergency_call: true
				}
			rescue => e
				if e.message.include?('System maintenance') ||
						e.message.include?('System abnormality')
					@binance_fee_map = {}
					@can_deposit_map = {}
					@can_withdraw_map = {}
					return true
				end
				raise e
			end
			err = APD::ExpireResultFailed.new("Binance API failed.")
			raise err if res.nil?
			raise "response error #{res}" if res.nil? || res.is_a?(Array) == false

			can_deposit_map, can_withdraw_map, fee_map = {}, {}, {}
			binance_fee_by_network, can_deposit_by_network, can_withdraw_by_network = {}, {}, {}
			res.each { |asset_info|
				coin = asset_info['coin']
				next if coin =~ /^.*(DOWN|UP)$/ # ADADOWN AAVEUP
				asset = coin.upcase
				info_found = false
				is_fiat = (asset_info['isLegalMoney'] == true)
				network_num = asset_info['networkList'].size
				asset_info['networkList'].each { |net_info|
					is_fiat = true if net_info['network'] == 'FIAT_MONEY'
					# Save side network info as well.
					network = net_info['network']
					binance_fee_by_network[asset] ||= {}
					binance_fee_by_network[asset][network] = net_info['withdrawFee'].to_f # "withdrawFee": "0.005"
					can_deposit_by_network[asset] ||= {}
					can_deposit_by_network[asset][network] = net_info['depositEnable'] # "depositEnable": false,
					can_withdraw_by_network[asset] ||= {}
					can_withdraw_by_network[asset][network] = net_info['withdrawEnable'] # "withdrawEnable": false,

					if net_info['network'] == coin
						# Mainnet
					elsif net_info['network'] =~ /^[A-Z0-9]{3,5}$/ && net_info['coin'] == coin && (net_info['isDefault'] == true || network_num == 1)
						# ETH BSC ONT ... Token, check default network only
						# BOBA said its only network is not default, but we still choose the only one
					else
						next
					end
					fee_map[asset] = net_info['withdrawFee'].to_f # "withdrawFee": "0.005", 
					can_deposit_map[asset] = net_info['depositEnable'] # "depositEnable": false,
					can_withdraw_map[asset] = net_info['withdrawEnable'] # "withdrawEnable": false,
					info_found = true
				}
				next if is_fiat
				# puts "No withdraw/deposit info for #{asset} #{JSON.pretty_generate(asset_info)}" if @verbose && info_found != true
			}
			@binance_fee_map = fee_map
			@can_deposit_map = can_deposit_map
			@can_withdraw_map = can_withdraw_map
			@binance_fee_by_network, @can_deposit_by_network, @can_withdraw_by_network = binance_fee_by_network, can_deposit_by_network, can_withdraw_by_network
			return true
		end
		expire_every MARKET_ASSET_FEE_CACHE_T, :binance_asset_info

		def support_deviation?(pair)
			true
		end

		# FEE*3/4 with BNB, should be cached in valid time.
		def fee_rate_real_evaluate(pair, opt={})
			verbose = @verbose && opt[:verbose] != false
			@_fee_rate_real ||= {}
			json = nil
			new_opt = opt.clone
			new_opt[:allow_fail] = true # Always request with allow_fail:true
			new_opt[:weight] = 10
			new_opt[:emergency_call] = true

			binance_symbol_info() # Reload all trading fee by symbols.
			# Account has universal trading fee data.
			json = redis_cached_call("V3Account", 60, new_opt) {
				binance_req '/api/v3/account', new_opt
			}

			tk_comm = mk_comm = nil
			# BUSD pairs seems have wrong fee data in @binance_trading_fee
			# Use USDT-BTC fee as universal fee instead.
			if false && @binance_trading_fee[pair] != nil
				# Load from specific pair fee data
				mk_comm = @binance_trading_fee.dig(pair, 'maker')
				tk_comm = @binance_trading_fee.dig(pair, 'taker')
			elsif false && @binance_trading_fee[pair_to_underlying_pair(pair)] != nil
				# Load from specific pair fee data.
				mk_comm = @binance_trading_fee.dig(pair_to_underlying_pair(pair), 'maker')
				tk_comm = @binance_trading_fee.dig(pair_to_underlying_pair(pair), 'taker')
			elsif @binance_trading_fee['USDT-BTC'] != nil
				# Load from USDT-BTC fee data.
				mk_comm = @binance_trading_fee.dig('USDT-BTC', 'maker')
				tk_comm = @binance_trading_fee.dig('USDT-BTC', 'taker')
			elsif json != nil
				# Load from universal fee data.
				mk_comm = (json['makerCommission'].to_f/10000.0).round(8)
				tk_comm = (json['takerCommission'].to_f/10000.0).round(8)
			elsif opt[:allow_fail] == true
				# Cache failed error.
				raise APD::ExpireResultFailed.new
			else
				# Fallback to highest fee.
				tk_comm = mk_comm = 0.1/100
			end
			# 75~78% BNB discount only applied when maker fee > 0
			cash, asset = pair.split('-')
			if mk_comm > 0 # Discount applies on positive fee only.
				map = {
					'maker/buy' => mk_comm*0.78,
					'maker/sell' => mk_comm*0.78,
					'taker/buy' => tk_comm*0.78,
					'taker/sell' => tk_comm*0.78
				}
			else
				map = {
					'maker/buy' => mk_comm,
					'maker/sell' => mk_comm,
					'taker/buy' => tk_comm,
					'taker/sell' => tk_comm
				}
			end
			map = map.to_a.map { |kv| [kv[0], kv[1].round(10)] }.to_h
			@_fee_rate_real[pair] = map
		end
		expire_every MARKET_ASSET_FEE_CACHE_T, :fee_rate_real_evaluate

		def price_step(pair)
			pair = get_active_pair(pair)
			raise "Pair should not be nil" if pair.nil?
			info = binance_symbol_info(pair)
			filter = info['filters'].select { |f| f['filterType'] == 'PRICE_FILTER' }.first
			raise "No such PRICE_FILTER for #{pair}" if filter.nil?
			return filter['tickSize'].to_f
		end

		def quantity_step(pair)
			pair = get_active_pair(pair)
			raise "Pair should not be nil" if pair.nil?
			info = binance_symbol_info(pair)
			filter = info['filters'].select { |f| f['filterType'] == 'LOT_SIZE' }.first
			raise "No such LOT_SIZE for #{pair}" if filter.nil?
			return filter['stepSize'].to_f
		end

		def min_quantity(pair)
			pair = get_active_pair(pair)
			raise "Pair should not be nil" if pair.nil?
			info = binance_symbol_info(pair)
			filter = info['filters'].select { |f| f['filterType'] == 'LOT_SIZE' }.first
			raise "No such LOT_SIZE for #{pair}" if filter.nil?
			return filter['minQty'].to_f
		end

		def min_vol(pair)
			pair = get_active_pair(pair)
			raise "Pair should not be nil" if pair.nil?
			info = binance_symbol_info(pair)
			filter = info['filters'].select { |f| f['filterType'] == 'MIN_NOTIONAL' }.first
			raise "No such MIN_NOTIONAL for #{pair}" if filter.nil?
			return (filter['minNotional'].to_f+SATOSHI)
		end

		def price_ratio_range(pair)
			info = binance_symbol_info(pair)
			filter = info['filters'].select { |f| f['filterType'] == 'PERCENT_PRICE' }.first
			raise "No such PERCENT_PRICE for #{pair}" if filter.nil?
			up = filter['multiplierUp'] || raise("No multiplierUp in PERCENT_PRICE for #{pair}")
			down = filter['multiplierDown'] || raise("No multiplierDown in PERCENT_PRICE for #{pair}")
			[down.to_f*1.1, up.to_f*0.9] # Binance uses avg price to determine bound, so apply with 90% 110%
		end

		def binance_symbol_info(pair=nil)
			if @binance_symbol_info.nil?
				# https://api.binance.com/api/v3/exchangeInfo
				# Cache: /res/binance_symbols.json Also required by OMS
				res = nil
				begin
					puts "Try to load trading rules from webpage"
					res = redis_cached_call("V3ExchangeInfo", 600) {
						file_cached_call('v3_exchangeInfo', call_first: true) {
							binance_req '/api/v3/exchangeInfo', public:true, allow_fail:true, weight: 10, emergency_call: true, skip_api_rate_control: true
						}
					}
				rescue => e
					APD::Logger.error e
					puts "Unknown error, try loading trading rules from file."
					res = file_cached_call('v3_exchangeInfo') { next nil }
				end

				# Parse rate limit
				weight_limit = res['rateLimits'].select { |l| l['rateLimitType'] == "REQUEST_WEIGHT" }
				raise "REQUEST_WEIGHT tuple num != 1 #{JSON.pretty_generate(res['rateLimits'])}" if weight_limit.size != 1
				weight_limit = weight_limit.first
				if weight_limit['interval'] == "SECOND"
					@binance_weight_limits = {
						'limit' => weight_limit['limit'],
						'second' => weight_limit['intervalNum']
					}
				elsif weight_limit['interval'] == "MINUTE"
					@binance_weight_limits = {
						'limit' => weight_limit['limit'],
						'second' => (weight_limit['intervalNum'] * 60)
					}
				else
					raise "Unknown interval #{weight_limit}"
				end

				# Parse order limit
				order_limit = res['rateLimits'].select { |l| l['rateLimitType'] == "ORDERS" && l['interval'] != "DAY" }
				raise "ORDERS tuple num != 1 #{JSON.pretty_generate(res['rateLimits'])}" if order_limit.size != 1
				order_limit = order_limit.first
				if order_limit['interval'] == "SECOND"
					@binance_order_limits = {
						'limit' => order_limit['limit'],
						'second' => order_limit['intervalNum']
					}
				elsif order_limit['interval'] == "MINUTE"
					@binance_order_limits = {
						'limit' => order_limit['limit'],
						'second' => (order_limit['intervalNum'] * 60)
					}
				else
					raise "Unknown interval #{weight_limit}"
				end

				# Parse symbols.
				res = res['symbols'].
					map { |r| [r['symbol'], r] }.
					to_h
				res[binance_symbol('BTC-GLM')] ||= res[binance_symbol('BTC-GNT')]
				res[binance_symbol('ETH-GLM')] ||= res[binance_symbol('ETH-GNT')]
				pair_symbol, symbol_pair = {}, {}
				res.each { |symbol, info|
					asset, currency = info['baseAsset'], info['quoteAsset']
					p = "#{currency}-#{asset}"
					pair_symbol[p] = symbol
					symbol_pair[symbol] = p
				}
				@binance_symbol_info = res
				@binance_symbol_pair = symbol_pair
				@binance_pair_symbol = pair_symbol
			end

			# Load trading fee by symbol
			if @binance_trading_fee.nil?
				begin
					puts "Try to load trading fee from webpage"
					res = redis_cached_call("trading_fee", 600) {
						file_cached_call('trading_fee', call_first: true) {
							res = binance_req '/sapi/v1/asset/tradeFee', method: :GET, allow_fail:true, weight: 1
						}
					}
				rescue => e
					APD::Logger.error e
					puts "Unknown error, try loading trading fee from file."
					res = file_cached_call('trading_fee') { next nil }
				end
				trading_fee_map = {}
				res.each { |d|
					# "symbol": "1INCHBTC",
					# "makerCommission": "0.0009",
					# "takerCommission": "0.001"
					sym = d['symbol']
					p = @binance_symbol_pair[sym]
					if p.nil?
						puts "No pair for #{sym} in trading_fee data #{d}"
						next
					end
					mk_fee, tk_fee = d['makerCommission'], d['takerCommission']
					if mk_fee.nil? || tk_fee.nil?
						puts "No fee for #{sym} in trading_fee data #{d}"
						next
					end
					trading_fee_map[p] = {
						'maker' => mk_fee.to_f.round(8),
						'taker' => tk_fee.to_f.round(8)
					}
				}
				@binance_trading_fee = trading_fee_map
			end

			return @binance_symbol_info if pair.nil?
			ret = @binance_symbol_info[binance_symbol(pair)]
			raise "symbol info is not exist for #{pair}" if ret.nil?
			ret
		end
		expire_every MARKET_PAIRS_CACHE_T, :binance_symbol_info

		def balance(opt={})
			binance_symbol_info()
			verbose = @verbose && opt[:verbose] != false

			if opt[:skip_oms] != true
				oms_bal = oms_balance(opt)
				if oms_bal != nil
					balance_cache_write oms_bal
					balance_cache_print if verbose
					return @balance_cache
				else
					puts "Failed in fetch OMS balance".red
				end
			end

			opt[:weight] = 10
			opt[:emergency_call] = true
			json = binance_req '/api/v3/account', opt
			return nil if opt[:allow_fail] == true && json.nil?
			json = json['balances']
			cache = json.map do |r|
				[r['asset'].upcase, {'cash'=>r['free'].to_f, 'reserved'=>r['locked'].to_f}]
			end.select { |r| r[1]['cash'] != 0 || r[1]['reserved'] != 0 }.to_h
			balance_cache_write cache
			balance_cache_print if verbose
			@balance_cache
		end

		def generate_client_oid(o)
			s = o['client_oid']
			if s.nil?
				# a-zA-Z0-9-_ {1,36}
				s = "#{o['pair']}_#{(Time.now.to_f*1_000_000_000).to_i}_#{rand(9999)}"
				puts "client order id: #{s} generated"
			else
				puts "client order id: #{s} exists"
			end
			s
		end

		include URN::EmailUtil # For bug reporting.
		def place_order(pair, order, opt={})
			order = order.clone
			return nil if pre_place_order(pair, order) == false
			side = nil
			case order['T']
			when 'buy'
				side = 'BUY'
			when 'sell'
				side = 'SELL'
			else
				raise "Unknown order type."
			end
			client_oid = generate_client_oid(order)
			args = {
				:newClientOrderId => client_oid,
				:symbol		=> binance_symbol(pair),
				:side			=> side,
				:type			=> 'LIMIT',
				:timeInForce	=> 'GTC',
				:quantity	=> format_size_str(pair, order['T'], order['s'], adjust:true, verbose:true),
				:price		=> format_price_str(pair, order['T'], order['p'], verbose:true)
			}
			args[:timeInForce] = 'IOC' if opt[:tif] == 'IOC'
			args[:timeInForce] = 'FOK' if opt[:tif] == 'FOK'
			if opt[:tif] == 'PO'
				args[:type] = 'LIMIT_MAKER'
				args.delete(:timeInForce)
			end
			json, place_time_i = nil, nil

			order_priority = 9
			order_priority = 1 if pair =~ /^ETH-/ # Less important
			order_priority = 1 if opt[:priority] == :low
			begin # place_time_i might be changed
				place_time_i = (Time.now.to_f - 2) * 1000 # ms
				json = _async_operate_order(pair, client_oid, :new) {
					binance_req(
						'/api/v3/order', place_order:true, args:args, method: :POST,
						allow_fail:opt[:allow_fail], weight: 1,
						order_priority: order_priority,
						emergency_call:opt[:emergency_call]
					)
				}
				return nil if json.nil? && opt[:allow_fail] == true
				raise OrderMightBePlaced.new if opt[:test_order_might_be_place] == true
			rescue OrderAlreadyPlaced, OrderMightBePlaced => e
				cost_s = (Time.now.to_f - place_time_i/1000 - 2)
				sleep_s = [60 - cost_s, 5].max
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
			query_o = order.clone
			query_o['i'] = json['orderId'].to_s
      query_o['client_oid'] = client_oid
			query_o['s'] = args[:quantity].to_f
			query_o['t'] = place_time_i
			raise "No order id" if query_o["i"].nil?
			# Once the order id got, should query the order without allow_fail option.
			# Binance_BUG01 : sometimes it returns wrong order while querying new placed order id.
			# Verify, not trust it.
			trade = nil
			loop {
				trade = query_order pair, query_o, just_placed:true
				if trade['p'].round(8) == query_o['p'].round(8) &&
					trade['s'].round(8) == query_o['s'].round(8) && 
					trade['t'] >= place_time_i
					break
				elsif trade['p'].round(8) == query_o['p'].round(8) &&
					trade['t'] >= place_time_i
					# Sometimes Binance would change its size, alert w/ email
					diff = query_o['s'] - trade['s']
					puts "Binance BUG01, order size changed #{trade.to_json}\n-> #{query_o.to_json}"
					if URN::REPORT_RECIPIENT != nil
						title = "#{market_name()} #{pair} order size bug #{query_o['s']} -> #{trade['s']}"
						content = [
							"args: #{args}",
							"order: #{query_o.to_json}",
							"trade: #{trade.to_json}",
							format_trade(query_o).uncolorize,
							format_trade(trade).uncolorize
						]
						puts content.join("\n")
						email_plain URN::REPORT_RECIPIENT, title, content.join("\n<p/>")
					end
					break
				else
					info = [trade['p'], query_o['p'], trade['s'], query_o['s'], trade['t'], place_time_i]
					puts "Binance_BUG01 triggered #{info}, clear cache and retry querying after 1s"
					if URN::OMSLocalCache.support_mkt?(market_name())
						URN::OMSLocalCache.oms_delete(market_name(), client_oid)
						URN::OMSLocalCache.oms_delete(market_name(), query_o['i'])
					end
					keep_sleep 1
				end
			}
			post_place_order(trade)
			trade
		end

		def query_order(pair, order, opt={})
			verbose = @verbose && opt[:verbose] != false
			order['pair']	= pair
			# Query by order id by default. But:
			# 	binance order id might be duplicated in OMS for different orders
			# 	binance order OMS following update might not come with client_oid after first update.
			# Above are reasons of Binance_BUG01
			oms_id = order['i'] || order['client_oid']
			json = oms_order_info(pair, oms_id, opt)
			oms_missed = false
			if json.nil?
				oms_missed = true
				args = nil
				if order['i'].nil?
					puts ">> #{pair} client_oid #{order['client_oid']}" if verbose
					args = {
						:symbol		=> binance_symbol(pair),
						:origClientOrderId => order['client_oid']
					}
				else
					puts ">> #{pair} #{order['i']}" if verbose
					args = {
						:symbol		=> binance_symbol(pair),
						:orderId	=> order['i']
					}
				end
				wait_gap = 1
				begin
					emergency_call = oms_enabled?()
					if opt[:just_placed]
						json = _async_operate_order(pair, oms_id, :query_new) {
							binance_req '/api/v3/order', args:args, allow_fail:opt[:allow_fail], silent:opt[:silent], weight: 2, emergency_call: emergency_call
						}
					else
						json = binance_req '/api/v3/order', args:args, allow_fail:opt[:allow_fail], silent:opt[:silent], weight: 2, emergency_call: emergency_call
					end
				rescue OrderNotExist => e
					puts "No order found:\n#{format_trade(order)}"
					if opt[:just_placed] # Sometimes order args have ['t'] too, cloned from other orders.
						;
					elsif order['t'] != nil && order_age(order) >= 2*3600_000
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
			end
			trade = binance_normalize_trade pair, json
			oms_order_write_if_null(pair, oms_id, trade) if oms_missed
			post_query_order(order, trade, opt)
			trade
		end

		def cancel_order(pair, order, opt={})
			return cancel_canceling_order(pair, order, opt) if order['status'] == 'canceling'
			return nil if pre_cancel_order(order) == false
			args = {
				:symbol		=> binance_symbol(pair),
				:orderId	=> order['i']
			}
			begin
				json = _async_operate_order(pair, order['i'], :cancel) {
					binance_req '/api/v3/order', args:args, method: :DELETE, allow_fail:opt[:allow_fail], cancel_order:true, weight: 1
				}
				return nil if json.nil? && opt[:allow_fail] == true
			rescue URN::OrderNotExist => e
				puts "Order could not be located, maybe it is cancelled already.".red
				oms_order_delete(pair, order['i'])
			end
			record_operation_time

			trade = oms_wait_trade(pair, order['i'], 1, :cancel, force_update:order)
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
			json = nil
			args = {}
			weight = 3
			if pair.nil?
				weight = 40
			else
				args[:symbol] = binance_symbol(pair)
			end
			json = binance_req '/api/v3/openOrders', args:args, allow_fail:opt[:allow_fail], weight: weight, silent:opt[:silent]
			return nil if json.nil? && opt[:allow_fail] == true
			orders = json.map do |o|
				o = binance_normalize_trade(pair, o)
				print "#{o['pair']}\n" if verbose && pair.nil?
				print "#{format_trade(o)}\n" if verbose
				o
			end
			orders
		end

		def history_orders(pair, opt={})
			verbose = @verbose && opt[:verbose] != false
			json = nil
			raise "pair should be specified." if pair.nil?
			args = {:symbol => binance_symbol(pair)}
			json = binance_req '/api/v3/allOrders', args:args, allow_fail:opt[:allow_fail], weight: 10, silent:opt[:silent]
			return nil if json.nil? && opt[:allow_fail] == true
			orders = json.map do |o|
				o = binance_normalize_trade(pair, o)
				print "#{o['pair']}\n" if verbose && pair.nil?
				print "#{format_trade(o)}\n" if verbose
				o
			end
			orders
		end

		#############################################
		# Withdraw and deposit
		# First, on Binance they don't have such an option, there is no such endpoint.
		# They don't have withdrawals in their API on Binance.
		# So, you have to do it manually until they implement a programmatic withdrawal endpoint.
		# We can't workaround that, unfortunately.
		#############################################
		def deposit_addr(asset, opt={})
			asset = asset.upcase
			args = { :coin => asset }
			args[:network] = 'BSC' if asset == 'BNB'
			args[:network] = 'TRX' if opt[:network] == 'TRON'
			args[:network] = 'BSC' if opt[:network] == 'BSC'
			his = nil
			begin
				res = binance_req '/sapi/v1/capital/deposit/address', args:args, method: :GET, allow_fail:opt[:allow_fail], weight: 1
				return nil if res.nil? && opt[:allow_fail] == true
			rescue ActionDisabled
				return nil
			end
			raise "No addr" if res.nil?
			addr = res['address']
			msg = res['tag']
			puts "#{asset} addr:[#{addr}]" if @verbose
			valid_addr?(addr)
			if msg != nil && msg.size > 0
				puts "#{asset} mesg:[#{msg}]" if @verbose
				valid_addr_msg?(msg)
				return [addr, msg]
			else
				return addr
			end
		end

		def withdraw(asset, amount, address, opt={})
			amount = amount.round(8)
			asset = asset.upcase

			# If amount is less than balance_cache but bank_balance has more,
			# Transfer required from bank.
			new_opt = opt.clone
			new_opt[:verbose] = false
			balance(new_opt) # Always query latest balance.
			return nil if @balance_cache.nil? && opt[:allow_fail] == true
			cash_bal = (@balance_cache.dig(asset, 'cash') || 0)
			if amount >= cash_bal
				puts "Not enough wallet balance #{cash_bal}, checking bank balance".red
				bank_balance(opt)
				return nil if @bank_balance.nil? && opt[:allow_fail] == true
				bank_bal = (@bank_balance.dig(asset, 'cash') || 0)
				if cash_bal + bank_bal <= amount
					raise NotEnoughBalance.new("Balance #{cash_bal} + Bank #{bank_bal} <= #{amount}")
				end
				need_transfer_amount = [(amount - cash_bal).ceil, bank_bal].min
				puts "Need redeem #{need_transfer_amount} from bank to wallet, start in 10 seconds".red
				keep_sleep 10
				bank_redeem_flexible_product(asset, need_transfer_amount, opt)
				# Keep sending if redeem failed, hope miracle would happen.
				start_t = Time.now.to_f
				puts "Refresh balance"
				loop {
					balance(new_opt)
					cash_bal = (@balance_cache.dig(asset, 'cash') || 0)
					passed_t = (Time.now.to_f - start_t).to_i
					puts "cash_bal #{cash_bal} passed_t #{passed_t}"
					break if cash_bal >= amount
					break if passed_t > 60
					keep_sleep 10
				}
			end

			args = {
				:coin		=> asset,
				:address=> address,
				:amount	=> amount
			}
			args[:network] = 'BSC' if asset == 'BNB'
			args[:network] = 'BSC' if opt[:network] == 'BSC'
			args[:network] = 'TRX' if opt[:network] == 'TRON'
			if opt[:message] != nil
				puts "Message: #{opt[:message]}".red
				args[:addressTag] = opt[:message]
			end
			if opt[:name] != nil
				puts "Name label: #{opt[:name]}".red
				args[:name] = opt[:name]
			end
			puts args
			puts "#{asset} WITHDRAW #{amount} -> #{address}".red
			valid_addr?(address)
			puts "Fire in 10 seconds".red
			keep_sleep 10
			res = binance_req '/sapi/v1/capital/withdraw/apply', args:args, method: :WITHDRAW, allow_fail:opt[:allow_fail], weight: 1, emergency_call: true
			puts JSON.pretty_generate(res)
			return nil if res.nil? && opt[:allow_fail] == true
			raise "response nil" if res.nil?
			if res['success'] != nil && res['success'] != true # No 'success' from sapi, wapi only.
				puts JSON.pretty_generate(res).red
				if res['msg'] != nil && res['msg'].include?('-4019=The current currency is not open for withdrawal')
					raise ActionDisabled.new(err_res)
				end
				raise res.to_json
			end
			raise res.to_json if res['id'].nil?
			res['id']
		end

		def transactions(asset, opt={})
			asset = asset.upcase
			limit = opt[:limit] || 3
			his = nil
			loop do
				his1 = transactions_int(asset, type:'deposit', allow_fail:true, silent:opt[:silent]) || []
				his2 = transactions_int(asset, type:'withdraw', allow_fail:true, silent:opt[:silent]) || []
				his = (his1+his2).map do |r|
					r['asset'] = asset
					r['type'] = r.delete('T')
					r['txid'] = r.delete('txId')
					r['t'] = r['insertTime'] || r['successTime'] || r['applyTime']
					r['t'] = DateTime.parse(r['t']).strftime('%Q').to_i if r['t'].is_a?(String)
					r['time'] = format_trade_time(r['t'])
					r['finished'] = (r['status'] == 'Success' || r['status'] == 'Credited' || r['status'] == 'Completed')
					r['rejected'] = ['Rejected', 'Cancelled'].include?(r['status'])
					r['finished'] = true if r['type'] == 'withdraw' && r['txid'] != nil
					r['finished'] ||= (r['status'].nil? && r['txid'] != nil && r['successTime'] != nil)
					r['xfr_id'] = r.delete('id')
					r
				end.sort_by { |r| r['t'] }.reverse[0..(limit-1)]
				break unless opt[:watch] == true
				puts JSON.pretty_generate(his)
				keep_sleep 10
			end
			his
		end

		def transactions_int(asset, opt={})
			withdraw_status_map = {
				0=>'Email Sent',
				1=>'Cancelled',
				2=>'Awaiting Approval',
				3=>'Rejected',
				4=>'Processing',
				5=>'Failure',
				6=>'Completed'}
			# 0:pending,6: credited but cannot withdraw, 1:success
			deposit_status_map = {
				0=>'Pending',
				1=>'Success',
				6=>'Credited'}
			type = opt[:type]
			path = nil
			status_map = nil
			if type == 'deposit'
				path = '/sapi/v1/capital/deposit/hisrec'
				status_map = deposit_status_map
			elsif type == 'withdraw'
				path = '/sapi/v1/capital/withdraw/history'
				status_map = withdraw_status_map
			else
				raise "Unsupported type:#{type}"
			end
			args = { :coin => asset }
			res = binance_req path, args:args, method: :GET, allow_fail:opt[:allow_fail], silent:opt[:silent], weight: 1
			return nil if res.nil? && opt[:allow_fail] == true
			list = res[0..9].map do |r|
				r['T'] = type
				r['amount'] = r['amount'].to_f
				r['status'] = status_map[r['status']]
				r
			end
			list
		end

		def market_summaries(opt={})
			res = redis_cached_call("market_summaries", 600) {
				binance_req '/api/v3/ticker/24hr', public:true, allow_fail: true, weight: 40
			}
			return nil if res.nil? && opt[:allow_fail] == true
			binance_symbol_info() if @binance_symbol_info.nil?
			result = {}
			res.each { |r|
# 				"symbol": "GALTRY",
# 				"priceChange": "-28.10000000",
# 				"priceChangePercent": "-11.007",
# 				"weightedAvgPrice": "233.93157544",
# 				"prevClosePrice": "0.00000000",
# 				"lastPrice": "227.20000000",
# 				"lastQty": "4.38900000",
# 				"bidPrice": "227.40000000",
# 				"bidQty": "10.00000000",
# 				"askPrice": "229.90000000",
# 				"askQty": "6.53900000",
# 				"openPrice": "255.30000000",
# 				"highPrice": "269.00000000",
# 				"lowPrice": "211.60000000",
# 				"volume": "27063.83500000",
# 				"quoteVolume": "6331085.55900000",
# 				"openTime": 1651764577451,
# 				"closeTime": 1651850977451,
				pair = binance_standard_pair(r['symbol'])
				result[pair] = {
					'from'					=> r['openTime'].to_i,
					'open'					=> r['openPrice'].to_f,
					'last'					=> r['lastPrice'].to_f,
					'high'					=> r['highPrice'].to_f,
					'low'						=> r['lowPrice'].to_f,
					'amt'						=> r['volume'].to_f,
					'vol'						=> r['quoteVolume'].to_f
				}
			}
			return result
		end

		def market_summary(pair, opt={})
			pair = pair_to_underlying_pair(pair.upcase)
			all_pairs(opt) if @binance_pair_symbol.nil?
			return nil if @binance_pair_symbol[pair].nil?
			args = {
				:limit		=> 2,
				:interval	=> '1d',
				:symbol		=> binance_symbol(pair)
			}
			opt = opt.clone
			opt[:public] = true
			opt[:args] = args
			opt[:weight] = 1
			res = redis_cached_call("klines_#{pair}", 600) {
				binance_req '/api/v3/klines', opt
			}
			return nil if res.nil? && opt[:allow_fail] == true
			raise "Failed in getting klines" if res.nil?
			# pair might be just listed, trading not started yet, duplicate candle.
			res[1] = res[0] if res != nil && res.size == 1
			raise "Market summary size is #{res.size}: #{JSON.pretty_generate(res)}" unless res.size == 2
			# time, open, high, low, close
			{
				'from'					=> DateTime.strptime(res[0][0].to_s, '%Q'),
				'open'					=> res[0][1].to_f,
				'last'					=> res[1][4].to_f,
				'high'					=> [res[0][2].to_f, res[1][2].to_f].max,
				'low'						=> [res[0][3].to_f, res[1][3].to_f].min,
				'amt'						=> res[0][5].to_f,
				'vol'						=> res[0][7].to_f
			}
		end

		######### Binance Bank #########
		def bank_product_list(opt={})
			args = {
				:status => 'SUBSCRIBABLE',
				:featured => 'ALL'
			}
			res = binance_req '/sapi/v1/lending/daily/product/list', args:args, method: :GET, allow_fail:opt[:allow_fail], weight: 1
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res)
			bank_products = {}
			res.each { |r|
				bank_products[r['productId']] = {
					'id' => r['productId'],
					'asset' => r['asset'].upcase,
					'annual_rate' => r['avgAnnualInterestRate'].to_f,
					'daily_rate' => r['dailyInterestPerThousand'].to_f/1000,
					'can_buy' => r['canPurchase'],
					'can_redeem' => r['canRedeem'],
					'min_purchase_amount' => r['minPurchaseAmount'].to_f,
					'total_capacity' => r['upLimit'].to_f,
					'user_capacity' => r['upLimitPerUser'].to_f,
					'available_capacity' => r['upLimit'].to_f - r['purchasedAmount'].to_f
				}
			}
			bank_products = bank_products.to_a.sort_by { |kv| kv[1]['annual_rate'] }.to_h
			if opt[:verbose] == true
				puts "Bank product list:"
				bank_products.each { |id, d|
					puts [
						d['asset'].ljust(5),
						(d['annual_rate']*100).round(1).to_s.rjust(3)+'%',
						(d['available_capacity']/1000).round.to_s.rjust(8)+'K',
						d['can_buy'] ? ['BUY'] : ' x '.red,
						d['can_redeem'] ? ['RDM'] : ' x '.red
					].join(' ')
				}
			end
			@bank_products = bank_products
		end
		expire_every MARKET_ASSET_FEE_CACHE_T, :bank_product_list

		def bank_balance(opt={})
			res = binance_req '/sapi/v1/lending/union/account', args:{}, method: :GET, allow_fail:opt[:allow_fail], weight: 1
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res)
			data = {}
			res['positionAmountVos'].each { |r|
				total = r['amount']
				data[r['asset']] = {
					'cash' => total.to_f,
					'reserved' => 0,
					'pending' => 0
				}
				# Could check token position for more details.
# 				args = { :asset => r['asset'] }
# 				res2 = binance_req(
# 					'/sapi/v1/lending/daily/token/position', args:args,
# 					method: :GET, allow_fail:opt[:allow_fail], weight: 1
# 				)
# 				raise "res2 should contains one data" if res2.size != 1
# 				res2 = res2.first
# 				# puts JSON.pretty_generate(res2)
# 				data[r['asset']] = {
# 					'cash' => res2['freeAmount'].to_f,
# 					'reserved' => 0,
# 					'pending' => 0
# 				}
			}
			@bank_balance = data
		end

		def bank_interest_his(asset, opt={})
			# 	[{
			# 		"asset": "ETC",
			# 		"interest": "0.13688400",
			# 		"lendingType": "REGULAR",
			# 		"productName": "ETC 14D (7%)",
			# 		"time": 1569391407000
			# 	},
			# 	{
			# 		"asset": "ETC",
			# 		"interest": "1.61905500",
			# 		"lendingType": "REGULAR",
			# 		"productName": "ETC 14D (7%)",
			# 		"time": 1568160162000
			# 	}]
			# REGULAR for Fixed products, DAILY for flexible
			args = {
				:lendingType => 'DAILY',
				:size => 100
			}
			args[:asset] = asset.upcase if asset != nil
			res = binance_req '/sapi/v1/lending/union/interestHistory', args:args, method: :GET, allow_fail:opt[:allow_fail], weight: 1
			return nil if res.nil? && opt[:allow_fail] == true
			# puts JSON.pretty_generate(res)
			res
		end

		def bank_purchase_flexible_product(asset, amount, opt={})
			asset = asset.upcase
			product = bank_product(asset, opt)
			return nil if product.nil? && opt[:allow_fail] == true
			min_purchase_amount = product['min_purchase_amount']
			return nil if amount < min_purchase_amount
			puts "Available product for #{asset} : #{JSON.pretty_generate(product)}"

			args = {
				:productId => product['id'],
				:amount    => amount.to_f
			}
			puts "Will purchase #{args} opt: #{opt}".red
			begin
				res = binance_req '/sapi/v1/lending/daily/purchase', args:args, method: :POST, allow_fail:opt[:allow_fail], weight: 1
				return nil if res.nil? && opt[:allow_fail] == true
			rescue => e
				return nil if opt[:allow_fail] == true
				raise e
			end
			puts res.to_json
			keep_sleep 5
			# Refresh bank balance again.
			bank_balance()
			res
		end

		def bank_product(asset, opt={})
			products = bank_product_list(opt)
			return nil if products.nil? && opt[:allow_fail] == true
			products = products.to_a.select { |kv| kv[1]['asset'] == asset }
			if products.empty?
				puts "No bank products for #{asset}"
				return nil if opt[:allow_fail] == true
				raise "No bank products for #{asset}"
			end

			raise "More than one bank products for #{asset}" if products.size > 1
			products.first[1] # [[ID, info] ... ]
		end

		def bank_redeem_flexible_product(asset, amount, opt={})
			asset = asset.upcase
			product = bank_product(asset, opt)
			return nil if product.nil? && opt[:allow_fail] == true

			amount = amount.to_f.round(8)
			min_purchase_amount = product['min_purchase_amount']

			retry_ct = 0
			begin
				return nil if amount < min_purchase_amount
				puts "Product for #{asset} #{amount} #{JSON.pretty_generate(product)}"

				args = {
					:productId => product['id'],
					:amount    => amount.to_f,
					:type      => "FAST" # NORMAL -> redeem next day.
				}
				puts "Will redeem #{args} opt: #{opt}"

				res = binance_req '/sapi/v1/lending/daily/redeem', args:args, method: :POST, allow_fail:opt[:allow_fail], weight: 1
				return nil if res.nil? && opt[:allow_fail] == true
			rescue NotEnoughBalance
				retry_ct += 1
				if retry_ct < 0
					amount -= min_purchase_amount
					keep_sleep 1
					retry
				else
					return nil if opt[:allow_fail] == true
					raise e
				end
			rescue => e
				return nil if opt[:allow_fail] == true
				raise e
			end
			puts res.to_json
			keep_sleep 5
			# Refresh bank balance again.
			bank_balance()
			res
		end

		def margin_status(opt={})
			ret = margin_balance(opt)
			return nil if ret.nil? && opt[:allow_fail] == true && @margin_balance.nil?
			# Only assume BTC used as collateral.
			btc_balance = (@margin_balance_cross.dig('BTC', 'cash') || 0) - (@margin_balance_cross.dig('BTC', 'borrowed') || 0)
			level = @margin_lv_cross # BTC value / borrowed value
			if level > 0
				leverage = 1/level
				risk_mv = btc_balance / level
			else
				leverage = 0
				risk_mv = 0
			end
			res = [{
				'name' => 'Cross',
				'asset' => 'BTC',
				'position' => risk_mv.to_f.round(8),
				'wallet_balance'	=> btc_balance.to_f.round(8),
				'risk_balance'	=> risk_mv.to_f.round(8),
				'lev' => leverage.round(4),
				'max_lev' => 0.6, # For BnnMgr.margin_status_desc()
				'lv' => @margin_lv_cross
			}]
			res + @margin_status_isolated.values()
		end
		
		def margin_balance(opt={})
			if opt[:clear_cache] == true
				redis_cached_call_reset("V1MarginAccount")
				redis_cached_call_reset("V1MarginIsolatedAccount")
			end
			verbose = @verbose && opt[:verbose] != false
			opt[:weight] = 1
			json = redis_cached_call("V1MarginAccount", 60, opt) {
				binance_req '/sapi/v1/margin/account', opt
			}
			return nil if opt[:allow_fail] == true && json.nil?
			@margin_lv_cross = json['marginLevel'].to_f.round(5)
			json = json['userAssets']
			cache = json.map do |r|
				[
					r['asset'].upcase,
					{
						'cash'=>r['free'].to_f,
						'reserved'=>r['locked'].to_f,
						'borrowed' => r['borrowed'].to_f + r['interest'].to_f
					}
				]
			end.select { |r| r[1].values.any? { |v| v != 0 } }.to_h
			@margin_balance_cross = JSON.parse(cache.to_json) # Deep clone

			opt[:weight] = 1
			json = redis_cached_call("V1MarginIsolatedAccount", 60, opt) {
				binance_req '/sapi/v1/margin/isolated/account', opt
			}
			return nil if opt[:allow_fail] == true && json.nil?
			margin_lv_isolated = {}
			margin_status_isolated = {}
			json['assets'].each { |pos|
				empty_bal = true
				quote = pos['quoteAsset']['asset']
				base = pos['baseAsset']['asset']
				['baseAsset', 'quoteAsset'].each { |key|
					r = pos[key]
					asset = r['asset'].upcase
					next if r['free'].to_f == 0 && r['locked'].to_f == 0 && r['borrowed'].to_f == 0 && r['interest'].to_f == 0
					cache[asset] ||= {
						'cash'=>0.0,
						'reserved'=>0.0,
						'borrowed' => 0.0
					}
					empty_bal = false
					cache[asset]['cash'] += r['free'].to_f
					cache[asset]['reserved'] += r['locked'].to_f
					cache[asset]['borrowed'] += (r['borrowed'].to_f + r['interest'].to_f)
				}
				next if empty_bal
				pair = pos['quoteAsset']['asset'] + '-' + pos['baseAsset']['asset']
				margin_lv_isolated[pair] = pos['marginLevel'].to_f.round(5)
				margin_status_isolated[pair] = {
					'name' => pair,
					'max_lev' => 0.6, # For BnnMgr.margin_status_desc()
					'lv' => pos['marginLevel'].to_f,
					'risk_balance' => pos['baseAsset']["netAssetOf#{quote.capitalize}"].to_f,
					'wallet_balance' => pos['quoteAsset']['netAsset'].to_f,
					'quote_balance' => pos['quoteAsset']['free'].to_f,
					'quote_locked' => pos['quoteAsset']['locked'].to_f,
					'quote_borrowed' => pos['quoteAsset']['borrowed'].to_f + pos['quoteAsset']['interest'].to_f,
					'base_balance' => pos['baseAsset']['free'].to_f,
					'base_locked' => pos['baseAsset']['locked'].to_f,
					'base_borrowed' => pos['baseAsset']['borrowed'].to_f + pos['baseAsset']['interest'].to_f,
					'raw' => pos
				}
				level = pos['marginLevel'].to_f
				quote_balance = pos['quoteAsset']['netAsset'].to_f
				if level > 0
					margin_status_isolated[pair]['lev'] = (1.0/level).round(4)
					margin_status_isolated[pair]['position'] = quote_balance / level
				else
					margin_status_isolated[pair]['lev'] = 0
					margin_status_isolated[pair]['position'] = 0
				end
			}
			@margin_status_isolated = margin_status_isolated
			@margin_lv_isolated = margin_lv_isolated

			@margin_balance = cache
			cache
		end

		# type = 1 : transfer from main account to margin account
		# type = 2 : transfer from margin account to main account
		def margin_transfer(asset, amount, type, opt={})

			asset = asset.upcase
			if @margin_asset_list.include?(asset)
				puts "Use cross margin to #{type} #{asset}"
				if type == 'out'
					fundout_max_amount = @margin_balance_cross.dig(asset, 'cash') || 0
					if fundout_max_amount == 0
						raise URN::NotEnoughBalance.new
					elsif amount > fundout_max_amount
						puts "Max amount reached, change amount to #{fundout_max_amount}".red
						amount = fundout_max_amount
					end
				end
			elsif @margin_asset_list_isolated.include?(asset)
				puts "Use isolated margin to #{type} #{asset}"
				return margin_isolated_transfer(collateral, pair, fundin_amount, 'in', allow_fail:opt[:allow_fail])
			else
				raise "Borrowing/Repaying #{asset} is not supported"
			end
			opt = opt.clone
			if type == 'in'
				type = 1
			elsif type == 'out'
				type = 2
			else
				raise "Unknown type #{type}"
			end
			opt[:args] = {
				:asset => asset,
				:amount => amount.round(8),
				:type => type
			}
			opt[:method] = :POST
			opt[:weight] = 1
			json = binance_req '/sapi/v1/margin/transfer', opt
			return nil if opt[:allow_fail] == true && json.nil?
			margin_balance(silent:true, clear_cache:true, allow_fail:true)
		end
		
		def margin_borrow_repay(asset, amount, type, opt={})
			asset = asset.upcase
			if @margin_asset_list_isolated.include?(asset) == false && @margin_asset_list.include?(asset) == false
				raise "Borrowing/Repaying #{asset} is not supported"
			elsif @margin_asset_list_isolated.include?(asset) && @margin_asset_list.include?(asset) == false
				return isolated_borrow_repay(asset, amount, type, opt={})
			end

			if type == 'repay'
				fundin_amount = amount - (@margin_balance_cross.dig(asset, 'cash') || 0)
				if fundin_amount > 0
					puts "Need to fundin #{asset} #{fundin_amount}"
					prepare_enough_balance(asset, fundin_amount)
					# keep_sleep 10 # Transfer in ASAP
					margin_transfer(asset, fundin_amount, 'in', opt)
					puts "Repay after 10s"
					keep_sleep 10
				end
			elsif type == 'borrow'
				limitation = margin_max_borrowable(asset, opt)
				return nil if limitation.nil? && opt[:allow_fail] == true
				if amount > limitation
					puts "Amount reduced to #{limitation} because of limitation"
					amount = limitation
				end
			end

			opt = opt.clone
			opt[:args] = {
				:asset => asset,
				:amount => amount.round(8)
			}
			if type == 'borrow'
				path = '/sapi/v1/margin/loan'
			elsif type == 'repay'
				path = '/sapi/v1/margin/repay'
			else
				raise "Unknown type #{type}"
			end
			opt[:method] = :POST
			opt[:weight] = 1
			if amount > 0
				json = binance_req path, opt
				return nil if opt[:allow_fail] == true && json.nil?
				puts "Refresh balance in 10 seconds"
				keep_sleep 10
				margin_balance(silent:true, clear_cache:true, allow_fail:true)
			end

			# Auto fundout after borrowing
			if type == 'borrow'
				fundout_amount = @margin_balance_cross.dig(asset, 'cash') || 0
				if fundout_amount < amount
					puts "So wired #{asset} balance < #{amount}, still try fundout".red
					fundout_amount = amount
				elsif fundout_amount > amount
					puts "Fundout all #{asset} balance #{fundout_amount} instead of #{amount}"
				end
				puts "Fundout #{fundout_amount} #{asset}".blue
				margin_transfer(asset, fundout_amount, 'out', allow_fail:true)
			else
				fundout_amount = @margin_balance_cross.dig(asset, 'cash') || 0
				if fundout_amount > 0
					puts "Fundout #{fundout_amount} #{asset}".blue
					margin_transfer(asset, fundout_amount, 'out', allow_fail:true)
				end
			end
			margin_balance(silent:true, clear_cache:true, allow_fail:true)
			ret = margin_status(allow_fail:true)
			return nil if ret.nil?
			puts "Cross margin status #{JSON.pretty_generate(ret)}"
		end

		def margin_isolated_transfer(asset, isolated_pair, amount, type, opt={})
			opt = opt.clone
			opt[:args] = {
				:asset => asset.upcase,
				:symbol => binance_symbol(isolated_pair),
				:amount => amount.round(8)
			}
			if type == 'in'
				opt[:args][:transFrom] = "SPOT"
				opt[:args][:transTo] = "ISOLATED_MARGIN"
			elsif type == 'out'
				opt[:args][:transFrom] = "ISOLATED_MARGIN"
				opt[:args][:transTo] = "SPOT"
			else
				raise "Unknown type #{type}"
			end
			opt[:method] = :POST
			opt[:weight] = 1
			json = binance_req '/sapi/v1/margin/isolated/transfer', opt
			return nil if opt[:allow_fail] == true && json.nil?
			margin_balance(silent:true, clear_cache:true, allow_fail:true)
		end
		
		# borrow flow:
		# 	compute safe collateral by 10x, fundin required BTC into BTC-ASSET account
		# 	borrow
		# 	auto fundout all ASSET from BTC-ASSET account
		# repay flow:
		# 	query all borrowed amount if amount=='all'
		# 	fundin required amount ASSET into BTC-ASSET account
		# 	repay
		# 	if no debt left, auto fundout all BTC from BTC-ASSET account
		def isolated_borrow_repay(asset, amount, type, opt={})
			asset = asset.upcase
			if @margin_asset_list_isolated.include?(asset) == false
				raise "Borrowing/Repaying #{asset} is not supported"
			elsif @margin_asset_list.include?(asset)
				raise "Use cross margin to #{type} #{asset}"
			end
			ret = margin_balance(silent:true, clear_cache:true, allow_fail:true)
			return nil if ret.nil? && opt[:allow_fail] == true

			collateral = 'BTC'
			pair = "#{collateral}-#{asset}"

			if type == 'borrow'
				amount = amount.round(8)
				limitation = margin_max_borrowable(asset, opt)
				return nil if limitation.nil? && opt[:allow_fail] == true
				if amount > limitation
					puts "Amount reduced to #{limitation} because of limitation".red
					amount = limitation
				end

				# Compute and fundin collateral assets.
				pair_price = market_summary(pair, allow_fail:opt[:allow_fail])
				return nil if pair_price.nil? && opt[:allow_fail] == true
				pair_price = pair_price['last']

				exist_collateral_balance = 0
				exist_asset_borrowed = 0
				status = @margin_status_isolated[pair]
				if @margin_status_isolated[pair].nil?
					puts "Current @margin_status_isolated[#{pair}] is empty.".blue
				else
					puts "Current @margin_status_isolated[#{pair}] is #{JSON.pretty_generate(status)}.".blue
					exist_collateral_balance = status['quote_balance']
					exist_asset_borrowed = status['base_borrowed']
				end
				collateral_amount = pair_price * (amount+exist_asset_borrowed) * 10
				puts "Safe collateral_amount is #{pair_price} x (#{amount} + #{exist_asset_borrowed}) x 5 = #{collateral_amount}.".blue
				if collateral_amount > exist_collateral_balance
					puts "#{collateral} exist balance #{exist_collateral_balance} < required #{collateral_amount}".red
					fundin_amount = (collateral_amount - exist_collateral_balance).ceil(1)

					puts "Fundin #{fundin_amount} #{collateral} into isolated_margin #{pair} in 10 seconds".red
					ret = margin_isolated_transfer(collateral, pair, fundin_amount, 'in', allow_fail:opt[:allow_fail])
					return nil if ret.nil? && opt[:allow_fail] == true
				else
					puts "#{collateral} exist balance #{exist_collateral_balance} > required #{collateral_amount}".green
				end
			elsif type == 'repay'
				if amount == 'all'
					amount = @margin_status_isolated.dig(pair, 'base_borrowed')
					puts "Get #{asset} loan amount #{amount}".blue
				else
					amount = amount.round(8)
				end

				if amount >= 0
					available_base_amount = @margin_status_isolated.dig(pair, 'base_balance')
					if available_base_amount >= amount
						puts "In #{pair}, available #{asset} amount #{available_base_amount} is enough to repay #{amount}".green
					else
						puts "In #{pair}, available #{asset} amount #{available_base_amount} is not enough".blue
						fundin_amount = (amount - available_base_amount).ceil(1)
						prepare_enough_balance(asset, fundin_amount)
						puts "Fundin #{fundin_amount} #{asset} into isolated_margin #{pair}".red
						# keep_sleep 10 # Transfer in ASAP
						ret = margin_isolated_transfer(asset, pair, fundin_amount, 'in', allow_fail:opt[:allow_fail])
						return nil if ret.nil? && opt[:allow_fail] == true
					end
				end
			end

			puts "#{type} #{amount} #{asset} from isolated account #{pair} after 10 seconds".blue
			keep_sleep 10
			opt = opt.clone
			opt[:args] = {
				:asset => asset,
				:isIsolated => true,
				:symbol => binance_symbol(pair),
				:amount => amount
			}
			if type == 'borrow'
				path = '/sapi/v1/margin/loan'
			elsif type == 'repay'
				path = '/sapi/v1/margin/repay'
			else
				raise "Unknown type #{type}"
			end
			opt[:method] = :POST
			opt[:weight] = 1
			if amount == 0
				puts "Skip sending #{type} API request because amount is zero".green
			else
				json = binance_req path, opt
				return nil if opt[:allow_fail] == true && json.nil?
				puts "Query margin balance in 10 seconds".blue
				keep_sleep 10
				margin_balance(silent:true, clear_cache:true, allow_fail:true)
			end

			if type == 'borrow'
				if amount > 0
					# Fundout automatically.
					fundout_amount = @margin_status_isolated.dig(pair, 'base_balance') || 0
					if fundout_amount < amount
						puts "So wired #{pair} #{asset} balance < #{amount}, still try fundout".red
						fundout_amount = amount
					elsif fundout_amount > amount
						puts "Fundout #{pair} all #{asset} balance #{fundout_amount} instead of #{amount}"
					end
					puts "Fundout #{fundout_amount} #{asset} in 10 seconds".blue
					keep_sleep 10
					ret = margin_isolated_transfer(asset, pair, fundout_amount, 'out', allow_fail:opt[:allow_fail])
					return nil if opt[:allow_fail] == true && ret.nil?
				end
			elsif type == 'repay'
				# Fundout all collateral from isolated account if no debt left.
				no_debt = true
				['base_borrowed', 'quote_borrowed', 'base_locked', 'quote_locked'].each { |k|
					b = @margin_status_isolated.dig(pair, k) || 0
					puts "margin_status_isolated #{pair} #{k} is #{b}".blue
					if (@margin_status_isolated.dig(pair, k) || 0) != 0
						puts "margin_status_isolated #{pair} #{k} is not zero".red
						no_debt = false
					end
				}
				if no_debt
					base_balance = @margin_status_isolated.dig(pair, 'base_balance') || 0
					if base_balance > 0
						puts "Fundout #{base_balance} #{asset} in 10 seconds".blue
						ret = margin_isolated_transfer(asset, pair, base_balance, 'out', allow_fail:opt[:allow_fail])
						puts "Failed in this step".red if ret.nil?
					end
					quote_balance = @margin_status_isolated.dig(pair, 'quote_balance') || 0
					if quote_balance > 0
						puts "Fundout #{quote_balance} #{collateral} in 10 seconds".blue
						ret = margin_isolated_transfer(collateral, pair, quote_balance, 'out', allow_fail:opt[:allow_fail])
						puts "Failed in this step".red if ret.nil?
					end
					puts "sleep 10 seconds before querying balance".red
					keep_sleep 10
				else
					puts "Skip fundout all from #{pair}".red
				end
			end

			# Clear cache and re-query
			margin_balance(silent:true, clear_cache:true, allow_fail:true)
			ret = margin_status(allow_fail:true)
			return nil if ret.nil?
			puts "Isolated #{pair} margin status #{JSON.pretty_generate(ret)}"
		end

		def margin_assets(opt={})
			if @margin_asset_list_isolated != nil && @margin_asset_list != nil
				return (@margin_asset_list_isolated + @margin_asset_list).uniq.sort
			end

			opt[:weight] = 1
			json = binance_req '/sapi/v1/margin/isolated/allPairs', opt
			return nil if opt[:allow_fail] == true && json.nil?
			@margin_asset_list_isolated = json.map { |r|
				[r['base'], r['quote']]
			}.reduce(:+).uniq

			json = binance_req '/sapi/v1/margin/allAssets', opt
			return nil if opt[:allow_fail] == true && json.nil?
			@margin_asset_list = json.select { |r| r['isBorrowable'] }.map { |r|
				r['assetName']
			}

			(@margin_asset_list_isolated + @margin_asset_list).uniq.sort
		end

		# Retuen max borrowable in cross margin account.
		# If asset only exist in isolated margin account, return max borrowable - borrowed in BTC-ASSET pair
		def margin_max_borrowable(asset, opt={})
			opt = opt.clone
			if margin_assets(opt).nil?
				return nil if opt[:allow_fail] == true && json.nil?
			end

			opt[:args] = { :asset => asset.upcase }
			opt[:weight] = 5
			isolated_margin_lv = nil
			isolated_asset_borrowed = nil
			if @margin_asset_list_isolated.include?(asset) && @margin_asset_list.include?(asset) == false
				pair = "BTC-#{asset}"
				if @margin_status_isolated.nil?
					if margin_balance(opt).nil?
						return nil if opt[:allow_fail] == true
					end
				end
				if @margin_status_isolated[pair].nil?
					isolated_margin_lv = 0 # No collateral yet.
				else
					isolated_margin_lv = @margin_status_isolated[pair]['lv']
					isolated_asset_borrowed = @margin_status_isolated[pair]['base_borrowed']
				end
				opt[:args][:isolatedSymbol] = binance_symbol(pair)
			end
			begin
				json = binance_req '/sapi/v1/margin/maxBorrowable', opt
				return nil if opt[:allow_fail] == true && json.nil?
			rescue NotEnoughBalance
				puts "The system does not have enough asset now"
				return 0
			rescue ActionDisabled
				puts "No cross margin available for #{asset}".red
				return 0
			end
			borrowable = json['amount'].to_f
			limit = json['borrowLimit'].to_f

			if isolated_margin_lv.nil?
				# Cross margin account
				return borrowable
			elsif isolated_margin_lv == 0
				# Isolated margin account with zero fund.
				return limit
			elsif isolated_margin_lv > 0
				# Isolated margin account with fund.
				return limit - isolated_asset_borrowed
			end
		end

		def _universal_transfer(asset, amount, type, opt={})
			asset = asset.upcase
			opt = opt.clone
			# TYPE:
			# MAIN_C2C Spot account transfer to C2C account
			# MAIN_UMFUTURE Spot account transfer to USDⓈ-M Futures account
			# MAIN_CMFUTURE Spot account transfer to COIN-M Futures account
			# MAIN_MARGIN Spot account transfer to Margin（cross）account
			# MAIN_MINING Spot account transfer to Mining account
			# C2C_MAIN C2C account transfer to Spot account
			# C2C_UMFUTURE C2C account transfer to USDⓈ-M Futures account
			# C2C_MINING C2C account transfer to Mining account
			# C2C_MARGIN C2C account transfer to Margin(cross) account
			# UMFUTURE_MAIN USDⓈ-M Futures account transfer to Spot account
			# UMFUTURE_C2C USDⓈ-M Futures account transfer to C2C account
			# UMFUTURE_MARGIN USDⓈ-M Futures account transfer to Margin（cross）account
			# CMFUTURE_MAIN COIN-M Futures account transfer to Spot account
			# CMFUTURE_MARGIN COIN-M Futures account transfer to Margin(cross) account
			# MARGIN_MAIN Margin（cross）account transfer to Spot account
			# MARGIN_UMFUTURE Margin（cross）account transfer to USDⓈ-M Futures
			# MARGIN_CMFUTURE Margin（cross）account transfer to COIN-M Futures
			# MARGIN_MINING Margin（cross）account transfer to Mining account
			# MARGIN_C2C Margin（cross）account transfer to C2C account
			# MINING_MAIN Mining account transfer to Spot account
			# MINING_UMFUTURE Mining account transfer to USDⓈ-M Futures account
			# MINING_C2C Mining account transfer to C2C account
			# MINING_MARGIN Mining account transfer to Margin(cross) account
			# MAIN_PAY Spot account transfer to Pay account
			# PAY_MAIN Pay account transfer to Spot account
			opt[:args] = {
				:asset => asset,
				:amount => amount.round(8),
				:type => type
			}
			opt[:method] = :POST
			opt[:weight] = 1
			json = binance_req '/sapi/v1/asset/transfer', opt
			return nil if opt[:allow_fail] == true && json.nil?
			json
		end

		######### WSS KEY #########
		def wss_key
			bal = balance(skip_oms:true, emergency_call: true, wss_key:true)
			res = binance_req '/api/v3/userDataStream', method: :WSSKEY, weight: 1, emergency_call: true, wss_key:true
			{
				'balance' => bal,
				'listenKey' => res['listenKey'],
				'header' => { :'X-MBX-APIKEY' => @BINANCE_API_KEY }
			}.to_json
		end

		def pair_to_underlying_pair(pair) # Dirty tricks to mapping pair -> trading pair
			if pair =~ /^USD-/
				pair = "BUSD-#{pair.split('-')[1]}"
			end
			pair
		end
		def underlying_pair_to_pair(pair) # Dirty tricks to mapping trading pair -> pair
			if pair =~ /^BUSD-/
				pair = "USD-#{pair.split('-')[1]}"
			end
			pair
		end

		private
		######### Format control #########
		def binance_symbol(pair)
			pair = pair_to_underlying_pair(pair.upcase)
			segs = pair.split('-')
			segs.reverse.join
		end
		def binance_standard_pair(symbol)
# 			pair = nil
# 			['BTC', 'ETH', 'USDT', 'USDC', 'BUSD', 'BNB', 'USD', 'TRX'].each do |base|
# 				if symbol.end_with?(base)
# 					end_pos = 0 - base.size - 1
# 					pair = (base + '-' + symbol[0..end_pos]).upcase
# 					break
# 				end
# 			end
# 			raise "Symbol #{symbol} should end with btc/eth/bnb/usd/usdt" if pair.nil?
			pair = @binance_symbol_pair[symbol]
			raise "Symbol #{symbol} does not exist" if pair.nil?
			pair = underlying_pair_to_pair(pair)
			pair
		end
		def binance_normalize_trade(pair, order)
			return order if order['_parsed_by_uranus'] == true
			order['i'] = order['orderId'].to_s
			order['client_oid'] = order.delete('clientOrderId')
			order['pair'] = pair || binance_standard_pair(order['symbol'])
			case (order['side'])
			when 'SELL'
				order['T'] = 'sell'
			when 'BUY'
				order['T'] = 'buy'
			else
				raise "Unknown order type: #{order.to_json}"
			end
			order['s'] = order['origQty'].to_f
			order['p'] = order['price'].to_f
			order['executed'] = order['executedQty'].to_f
			order['remained'] = order['s'] - order['executed']
			if order['type'] == 'LIMIT_MAKER'
				order['maker_size'] = order['s']
			else
				order['maker_size'] = order['remained']
			end
			case order['status']
			when 'NEW'
				order['status'] = 'new'
			when 'CANCELED'
				order['status'] = 'canceled'
			when 'EXPIRED'
				order['status'] = 'canceled'
			when 'FILLED'
				order['status'] = 'filled'
			when 'PARTIALLY_FILLED'
				order['status'] = 'new'
			when 'PENDING_CANCEL'
				order['status'] = 'canceling'
			when 'REJECTED'
				order['status'] = 'canceled'
			else
				raise "Unknown status #{order}"
			end
			order['t'] = order['time']
			order['market']	= 'Binance'
			order_status_evaluate(order)
			order
		end
		alias_method :_normalize_trade, :binance_normalize_trade
	end

	# Initialize client class with other API keys.
	ENV.keys.each { |env_k|
		next unless env_k =~ /^BINANCE_[A-Za-z]{1,2}_API_KEY$/
		mkt_code = env_k.split('BINANCE_')[1].split('_API_KEY')[0]
		next if ENV["BINANCE_#{mkt_code}_API_SEC"].nil?
		mkt_code = mkt_code.upcase # Y, C, F, G, VA ...
		mkt = "Binance_#{mkt_code}"
		next if URN.const_defined?(mkt)
		# puts "Initializing #{mkt}"
		clazz = Class.new(Binance)
		if ENV["BINANCE_#{mkt_code}_API_KEY"].nil? || ENV["BINANCE_#{mkt_code}_API_SEC"].nil?
			# puts "Skip init market #{mkt}".red
			next
		end
		clazz.instance_eval {
			define_method(:initialize) { |*args|
				opt = args[0] || {}
				instance_variable_set("@BINANCE_API_KEY".to_sym, ENV["BINANCE_#{mkt_code}_API_KEY"])
				instance_variable_set("@BINANCE_API_SEC".to_sym, ENV["BINANCE_#{mkt_code}_API_SEC"])
				super(opt)
			}
			alias_method(:binance_normalize_trade_original, :binance_normalize_trade)
			define_method(:binance_normalize_trade) { |pair, order|
				o = binance_normalize_trade_original(pair, order)
				o['market'] = mkt
				o
			}
			alias_method(:_normalize_trade, :binance_normalize_trade)
		}
		URN.const_set(mkt, clazz)
		TradeClient[mkt] = clazz
	}
end

######### CLI #########
if __FILE__ == $0 && defined? URN::BOOTSTRAP_LOAD
	mkt_code = (ARGV[0] || '').upcase
	if mkt_code =~ /^[A-Z]*$/ && URN.const_defined?('Binance_'+mkt_code)
		ARGV.shift
		client = URN.const_get('Binance_'+mkt_code).new verbose:true, skip_balance:true
	else
		client = URN::Binance.new verbose:true, skip_balance:true
	end
	client.run_cli
end
