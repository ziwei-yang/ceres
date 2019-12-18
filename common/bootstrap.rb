# Should load config before this file.
require_relative '../conf/config'
require_relative '../../aphrodite/common/bootstrap'

require 'parallel'
require 'rest-client'
require 'timeout'
require 'mail'
require 'gli'
require 'openssl'
require 'http'
require 'ecdsa'
require 'oj' if RUBY_ENGINE == 'ruby'
require 'securerandom'

module URN
	module Misc
		USE_OJ = RUBY_ENGINE == 'ruby'

		# Oj.load() has severe float precise problems
		# Suggest using JSON.parse for parsing managed JSON order status: such as progress
		def parse_json(str)
			if USE_OJ
				return Oj.load(str)
			else
				return JSON.parse(str)
			end
		end
	end
	module MathUtil
		def diff(f1, f2)
			f1 = f1.to_f
			f2 = f2.to_f
			return 9999999 if [f1, f2].min <= 0
			(f1 - f2) / [f1, f2].min
		end

		def format_num(f, float=8, decimal=8)
			return ''.ljust(decimal+float+1) if f.nil?
			return ' '.ljust(decimal+float+1, ' ') if f == 0
			return f.rjust(decimal+float+1) if f.is_a? String
			num = f.to_f
			f = "%.#{float}f" % f
			loop do
				break unless f.end_with?('0')
				break if f.end_with?('.0')
				f = f[0..-2]
			end
			segs = f.split('.')
			if num.to_i == num
				return "#{segs[0].rjust(decimal)} #{''.ljust(float, ' ')}"
			else
				return "#{segs[0].rjust(decimal)}.#{segs[1][0..float].ljust(float, ' ')}"
			end
		end
	end

	module OrderUtil
		include MathUtil

		# BTC-TRX@20190329 BTC-ETH@20190831@S
		def is_future?(pair)
			pair.split('@').size >= 2
		end

		def pair_assets(pair)
			segs = pair.split('-')
			raise "Invalid pair #{pair}" unless segs.size == 2
			asset1, asset2 = segs
			if is_future?(pair) # Each future pairs has different positions.
				return [asset1, pair]
			else
				return segs
			end
		end

		def format_order(o)
			o ||= {}
			return "#{'-'.ljust(5)}#{format_num(o['p'].to_f, 10, 4)}#{format_num(o['s'].to_f)}" if o['T'].nil?
			"#{o['T'].ljust(5)}#{format_num(o['p'].to_f, 10, 4)}#{format_num(o['s'].to_f)}"
		end

		def format_trade(t)
			return "Null trade" if t.nil?
			s = nil
			begin
				a = [
					"#{t['market']}#{t['account']}"[0..6].ljust(7),
			 		(t['T']||'-').ljust(4),
					format_num(t['p'], 10, 3),
					format_num(t['executed'], 3, 5) + '/' + format_num(t['s'], 3, 5),
					(t['status']||'-')[0..8].ljust(9), # canceling
					(t['i']||'-').to_s.ljust(6)[-6..-1].ljust(5),
					format_trade_time(t['t'])
				]
				if t['v'] != nil
					# Print size for volume based order.
					a[3] = format_num(t['executed_v'], 7, 1) + '/' + format_num(t['v'], 7, 1)
					a.push("S:#{format_num(t['s'], 4, 2)}")
					# Also print expiry if pair contains it.
					expiry = t['pair'].split('@')[1]
					if expiry == 'P'
						a.push "@P"
					else
						a.push("@#{expiry[-4..-1]}") unless expiry.nil?
					end
				end
				s = a.join(' ')
			rescue => e
				puts "Error in formatting:\n#{JSON.pretty_generate(t)}"
				raise e
			end
			if t['T'].nil?
				;
			elsif t['T'].downcase == 'sell' || t['T'].downcase == 'ask'
				s = s.red
			else
				s = s.green
			end
			s
		end

		def parse_trade_time(t)
			return 'no-time' if t.nil?
			DateTime.strptime(t.to_i.to_s, '%Q')
		end

		def format_trade_time(t)
			return 'no-time' if t.nil?
			t = t.to_i + 8*3600*1000
			DateTime.strptime(t.to_s, '%Q').strftime('%H:%M:%S %m/%d')
		end

		def format_millisecond(unix_mill)
			DateTime.strptime(unix_mill.to_s, '%Q').strftime('%Y%m%d %T.%4N')
		end

		def order_cancelled?(t)
			return t['_cancelled'] if t['_cancelled'] != nil
			t['_cancelled'] = order_cancelled_int?(t)
			t['_cancelled']
		end
		ORD_CANCELED_STATUS = ['canceled']
		def order_cancelled_int?(t) # include?() is slow.
			# Sometimes huobi remained size of canceled order could be zero because of latency.
			# Example: order filled in cancelling period
			# return false if t['remained'] == 0
			return false if t['status'].nil?
			ORD_CANCELED_STATUS.include?(t['status'].downcase)
		end

		ORD_NON_ALIVE_STATUS = [
			'filled',
			'canceled',
			'expired',
			'rejected'
		]
		def order_alive?(t)
			return t['_alive'] if t['_alive'] != nil
			t['_alive'] = order_alive_int?(t)
			t['_alive']
		end
		def order_alive_int?(t)
			return false if t['remained'] == 0
			return false if t['status'].nil?
			alive = ORD_NON_ALIVE_STATUS.include?(t['status'].downcase) == false
			t['_alive'] = alive
			alive
		end
		def order_canceling?(t)
			t['status'] == 'canceling'
		end
		def order_pending?(t)
			t['status'] == 'pending'
		end
		def order_set_dead(order)
			return unless order_alive?(order)
			order['executed'] ||= 0.0
			order['remained'] ||= (order['s']-order['executed'])
			if order['remained'] == 0
				order['status'] = 'filled'
			else
				order['status'] = 'canceled'
			end
			order['_alive'] = nil
			order['_cancelled'] = nil
			order_alive?(order)
			order_cancelled?(order)
			order
		end

		# Final touch of generating a usable order json
		# For non-volume based order, round its key values.
		# Then generate status cache.
		def order_status_evaluate(o)
			if o['v'].nil?
				# 3.3966055045870003
				# 0.00033031000000960375 => round(13)
				# 764.0000000000001 => round(12)
				# 741.000000000001 => round(11)
				# 764.00000000001 => round(10)
				['s', 'executed', 'remained', 'p', 'maker_size'].
					each { |k| o[k] = o[k].round(10) }
			end
			o['_alive'] = nil
			o['_cancelled'] = nil
			order_cancelled?(o)
			order_alive?(o)
		end

		# order age in ms
		def order_age(t)
			DateTime.now.strftime('%Q').to_i - t['t'].to_i
		end

		# Order is almost fully filled.
		def order_full_filled?(t, opt={})
			return true if t['remained'] == 0
			# omit size should be considered.
			omit_size = opt[:omit_size] || 0
			t['s'] - t['executed'] <= omit_size
		end

		def order_same?(o1, o2)
			return false if o1['market'] != o2['market']
			return false if o1['pair'] != o2['pair']
			if o1['T'] != nil && o2['T'] != nil
				return false if o1['T'] != o2['T']
			end
			# Some exchanges do not assign order id for instant filled orders.
			# We could identify them only by price-size-timestamp
			if o1['i'].to_s > '0' && o2['i'].to_s > '0'
				return o1['i'] == o2['i']
			end
			o1['s'] == o2['s'] && o1['p'] == o2['p'] && o1['t'] == o2['t']
		end

		def order_changed?(o1, o2)
			raise "Order are not same one:\n#{format_trade(o1)}\n#{format_order(o2)}" unless order_same?(o1, o2)
			return false if o1 == o2
			return true if o1['status'] != o2['status']
			return true if o1['executed'] != o2['executed']
			return true if o1['remained'] != o2['remained']
			return false
		end

		# Return true if o2 is newer than o1
		def order_should_update?(o1, o2)
			raise "Order are not same one:\n#{format_trade(o1)}\n#{format_order(o2)}" unless order_same?(o1, o2)
			case [order_alive?(o1), order_alive?(o2)]
			when [true, true]
				# Better replace if status: 'pending' -> 'new'
				if order_pending?(o1) && o1['executed'] <= o2['executed']
					return true
				end
				return o1['executed'] < o2['executed']
			when [true, false]
				return true
			when [false, true]
				return false
			when [false, false]
				return o1['executed'] < o2['executed']
			end
			raise "Should not reach here"
		end

		def order_stat(orders, opt={})
			precise = opt[:precise] || 1
			orders = [orders] if orders.is_a?(Hash)
			# Consider cancelled order's size as executed
			orders_size_sum = orders.map do |o|
				order_alive?(o) ? o['s'] : o['executed']
			end.reduce(:+) || 0
			orders_executed_sum = orders.map { |o| o['executed'] }.reduce(:+) || 0
			orders_remained_sum = (orders_size_sum - orders_executed_sum).round(precise)
			orders_executed_sum = orders_executed_sum.round(precise)
			orders_size_sum = orders_size_sum.round(precise)
			[orders_size_sum, orders_executed_sum, orders_remained_sum]
		end

		# Return real affected volume of order.
		# Fee would be deducted from vol for ask order.
		def order_real_vol(o)
			return 0 if o['executed'].nil?
			return 0 if o['executed'] == 0
			maker_size = o['maker_size'] || 0
			taker_size = o['executed'] - maker_size
			type = o['T']
			maker_fee = o['_data']['fee']["maker/#{type}"] || raise(JSON.pretty_generate(o))
			taker_fee = o['_data']['fee']["taker/#{type}"] || raise(JSON.pretty_generate(o))
			fee = o['p'] * maker_size * maker_fee + o['p'] * taker_size * taker_fee
			vol = o['p'] * o['executed']
			if type == 'buy'
				return vol + fee
			elsif type == 'sell'
				return vol - fee
			else
				raise "Unknown type #{type}"
			end
		end

		def order_same_mkt_pair?(orders)
			return true if orders.size < 2
			market_pair = [orders[0]['market'], orders[0]['pair']]
			same_market_pair = true
			orders.each do |o|
				if market_pair != [o['market'], o['pair']]
					same_market_pair = false
					break
				end
			end
			return same_market_pair
		end

		def _compute_real_price(p, rate, type)
			precise = @price_precise || 10
			if type == 'buy'
				return (p/(1-rate)).floor(precise)
			elsif type == 'sell'
				return (p*(1-rate)).ceil(precise)
			else
				raise "Unknown order type #{type}"
			end
		end

		def _compute_shown_price(p_real, rate, type)
			precise = @price_precise || 10
			if type == 'buy'
				return (p_real*(1-rate)).floor(precise)
			elsif type == 'sell'
				return (p_real/(1-rate)).ceil(precise)
			else
				raise "Unknown order type #{type}"
			end
		end
	end

	module CLI
		def terminal_width
			# IO.console.winsize
			# io-console does not support JRuby
			GLI::Terminal.new.size[0]
		end
		def terminal_height
			GLI::Terminal.new.size[1]
		end

		def get_input(opt={})
			puts(opt[:prompt].white.on_black, level:2) unless opt[:prompt].nil?
			timeout = opt[:timeout]
			if timeout.nil?
				return STDIN.gets.chomp
			elsif timeout == 0
				return 'Y'
			else
				ret = nil
				begin
					Timeout::timeout(timeout) do
						ret = STDIN.gets.chomp
					end
				rescue Timeout::Error
					ret = nil
				end
				return ret
			end
		end
	end

	module EmailUtil
		def email_plain(receiver, subject, content, bcc = nil, opt={})
			if opt[:skip_dup] != false
				@sent_emails ||= {}
				last_sent_t = @sent_emails["#{receiver}/#{subject}"]
				now = DateTime.now
				if last_sent_t != nil && (now - last_sent_t) < 1.0/24.0
					APD::Logger.highlight "Last t #{last_sent_t} skip sending same email:\n#{subject}\n#{content}"
					return
				end
				@sent_emails["#{receiver}/#{subject}"] = now
			end

			content = content.gsub(" ", '&nbsp;').gsub("\n", "<br>")
			loop do
				begin
					t = Thread.new do
						email_plain_int(receiver, subject, content, bcc, opt)
					end
					return t
				rescue ThreadError => e
					sleep 1
					retry if (e.message || '').include?('Resource temporarily unavailable')
					raise e
				end
			end
		end
		def email_plain_int(receiver, subject, content, bcc = nil, opt={})
			content ||= ""
			content += File.read(opt[:html_file]) unless opt[:html_file].nil?
			print "email_plain -> #{receiver} | #{subject} | content:#{content.size} attachment:#{opt[:file] != nil}\n"
			Mail.deliver do
				to      receiver
				from    'Uranus <uranus@uranus.com>'
				subject "#{subject} #{DateTime.now.strftime('%H:%M:%S')}"
				html_part do
					content_type 'text/html; charset=UTF-8'
					body content
				end
				unless opt[:file].nil?
					files = opt[:file]
					files = files.split(",") if files.is_a?(String)
					files = files.
						map { |f| f.strip }.
						select { |f| f.size > 0 }.
						sort.
						uniq
					files.each do |f|
						add_file f
					end
				end
			end
		end
	end

	# Fetch trader status from redis and conf files.
	module TraderStatusUtil
		include APD::CacheUtil
		include URN::OrderUtil
		def redis_db
			0
		end

		def trader_status(pair)
			pair = pair.upcase
			keys = redis.keys "URANUS:orders:#{pair}*"
			return {} if keys.empty?
			raise "More than one key for pair #{pair}: #{keys}" unless keys.size == 1
			key = keys.first
			fields = redis.hkeys(key)
			raise "More than one field for key #{key}" unless fields.size == 1
			f = fields.first
			raise "field not start with abt_union: #{f}" unless f.start_with?('abt_union')
			data = redis.hget key, f
			data = JSON.parse data
		end

		# Return pair list in /task/arbitrage_*
		def available_trading_pairs()
			pairs = Dir["#{URN::ROOT}/task/arbitrage_*.sh"].map do |f|
				File.basename(f).split('arbitrage_')[1].split('.sh')[0].upcase
			end
		end

		def active_task_exchanges
			Dir["#{URN::ROOT}/task/arbitrage_*.sh"].map do |f|
				line = File.read(f).
					split("\n").select { |l| l.include?('arbitrage_base.sh') }.
					select { |l| l.start_with?('#') == false }.first
				segs = line.split(' ').map { |m| m.gsub("'",'') }
				pair = "#{segs[2]}-#{segs[3]}"
				exchanges = segs[4..-1].map { |m| m.gsub("'",'').split('@').first }
				[pair, exchanges]
			end.to_h
		end

		def active_pairs(m)
			active_task_exchanges().to_a.
				select { |p_mlist| p_mlist[1].include?(m) }.
				map { |p_mlist| p_mlist[0] }
		end

		def active_exchanges(pair)
			base, asset = pair_assets(pair)
			file = "#{URN::ROOT}/task/arbitrage_#{asset.downcase}.sh"
			if base == 'BTC'
				;
			elsif base == 'USDT'
				file = "#{URN::ROOT}/task/arbitrage_usdt_#{asset.downcase}.sh"
			elsif base == 'USD'
				file = "#{URN::ROOT}/task/arbitrage_usd_#{asset.downcase}.sh"
			elsif base == 'ETH'
				file = "#{URN::ROOT}/task/arbitrage_eth_#{asset.downcase}.sh"
			else
				raise "No implemented"
			end
			return [] unless File.file?(file)
			File.read(file).
				split("\n").select { |l| l.include?('arbitrage_base.sh') }.
				select { |l| l.start_with?('#') == false }.first.
				split(" ")[4..-1].
				map { |m| m.gsub("'",'').split('@').first }
		end

		def active_spider_exchanges(pair)
			base, asset = pair_assets(pair)
			keyword = asset
			keyword = "#{base}-#{asset}" if base != 'BTC'
			file = "#{URN::ROOT}/bin/tmux_uranus.sh"
			File.read(file).
				split("\n").select { |l| l.include?('/spider.sh') }.
				map { |l| l.split('spider.sh')[1].gsub('"', '').strip }.
				select { |l| l.upcase.split(' ').include?(keyword.upcase) }.
				map { |l| l.split(' ').first }
		end
	end

	module CoinMarketUtil
		include APD::LogicControl
		include APD::SpiderUtil
		def coinmarket_stat_top99(opt={})
			return @coin_stat if opt[:cache] != false && @coin_stat != nil
			puts "Scanning all equities from coinmarketcap.com"
			url = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'
			response = nil
			endless_retry(sleep:5) { response = RestClient.get(url, accpet: :json) }
			ticker = JSON.parse response
			coin_stat = {}
			ticker.each do |t|
				symbol = t['symbol']
				symbol = 'IOTA' if symbol == 'MIOTA'
				if coin_stat[symbol] != nil
					# puts "Duplicate symbol in coinmarketcap #{symbol}"
					rank = t['rank'].to_i
					exist_rank = coin_stat[symbol]['rank'].to_i
					next if exist_rank < rank
				end
				t['price_sat'] = (t['price_btc'].to_f * 100000000).to_i
				coin_stat[symbol] = t
			end
			
			btc_p = coin_stat['BTC']['price_usd'].to_f
			coin_stat.each do |symbol, t|
				t['24h_volume_btc'] = (t['24h_volume_usd'].to_f/btc_p).to_i
			end
			@coin_stat = coin_stat
		end

		def coinmarket_stat(opt={})
			token_map = coinmarket_token_list(opt)
			coin_map = coinmarket_coin_list(opt)
			btc_price = coin_map.dig('BTC', 'price')
			raise "No BTC price." if btc_price.nil?
			map = {}
			# Merge and sort by market cap, evaluate rank again.
			(token_map.values + coin_map.values).
				sort_by { |d| d['market_cap'] }.
				reverse.each_with_index do |d, i|
					d['price_btc'] = d['price'].to_f/btc_price
					d['price_sat'] = (d['price_btc'].to_f * 100000000).to_i
					d['24h_volume_usd'] = d.delete('volume')
					d['24h_volume_btc'] = (d['24h_volume_usd'].to_f/btc_price).to_i
					d['rank'] = i+1
					map[d['name']] = d
				end
			map
		end

		def coinmarket_stat_web(opt={})
			# https://coinmarketcap.com/currencies/volume/24-hour/
		end

		# Parse currency list into coin_list
		# opt[:type] should be coin/token
		def _coinmarket_parse_list(url, coin_list, opt={})
			type = opt[:type]
			raise "Unknown opt[:type] #{type}" if ['token', 'coin'].include?(type) == false
			puts "Parsing coinmarketcap list #{url}"
			html = endless_retry(sleep:5) { parse_web(url) }
			html.clone.xpath("//table/tbody/tr").each do |line|
				cols = line.children.select { |c| c.name == 'td' }.
					map { |c| c.text.strip }
				if type == 'token' # Remove platform for token
					cols = cols[0..1] + cols[3..-1]
				end
				name = cols[5].split(' ')[1].strip
				name = 'IOTA' if name == 'MIOTA'
				if coin_list[name] != nil
					# puts "Duplicate coin name #{coin_list[name]['full_name']} - #{cols[1]}"
				elsif name =~ /^[A-Z0-9]{1,9}$/
					# Only accept ATOM:Cosmos BTT:BitTorrent
					next if name == 'ATOM' && (cols[1]||'').include?('Cosmos')==false
					next if name == 'BTT' && cols[1].include?('BitTorrent') == false
					coin_list[name] = {
						'type'	=> type,
						'rank'	=> cols[0].to_i,
						'name'	=> name,
						'full_name'	=> cols[1],
						'market_cap'	=> cols[2].gsub('$','').gsub(',','').gsub('*','').to_i,
						'price'	=> cols[3].gsub('$','').gsub(',','').gsub('*','').to_f,
						'volume'	=> cols[4].gsub('$','').gsub(',','').gsub('*','').to_f,
						'percent_change_24h'	=> cols[6].gsub('%','').gsub(',','').gsub('*','').to_f
					}
				else
					# puts "Unknown coin name #{name.inspect}"
				end
			end
		end
	
		def coinmarket_coin_list(opt={})
			return @_coinmarket_coin_list unless @_coinmarket_coin_list.nil?
			coin_list = {}
			(1..5).each do |i|
				url = "https://coinmarketcap.com/coins/#{i}/"
				_coinmarket_parse_list(url, coin_list, type:'coin')
			end
			@_coinmarket_coin_list = coin_list
			@_coinmarket_coin_list
		end
	
		def coinmarket_token_list(opt={})
			return @_coinmarket_token_list unless @_coinmarket_token_list.nil?
			token_list = {}
			(1..5).each do |i|
				url = "https://coinmarketcap.com/tokens/#{i}/"
				_coinmarket_parse_list(url, token_list, type:'token')
			end
			@_coinmarket_token_list = token_list
			@_coinmarket_token_list
		end
	end
end

unless defined? URN::BOOTSTRAP_LOAD_STARTED
	URN::BOOTSTRAP_LOAD_STARTED = true
	# Load market_client first.
	# puts "loading mkt.rb"
	require_relative './mkt'
	Dir["#{File.dirname(__FILE__)}/*.rb"].each do |f|
		next if File.basename(f).include?('legacy')
		# puts "loading #{f}"
		require_relative "./#{File.basename(f)}"
	end
	URN::ROOT = File.dirname(__FILE__) + '/../'
	URN::API_PROXY = (ENV['API_PROXY'] || 'default').
		split(',').
		map { |str| str=='default'?nil:str }
	# will create as many threads as necessary for work posted to it
	URN::CachedThreadPool = Concurrent::CachedThreadPool.new
	URN::BOOTSTRAP_LOAD = true
end
