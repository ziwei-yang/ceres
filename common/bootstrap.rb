# Should load config before this file.
require_relative '../conf/config'
require_relative '../../aphrodite/common/bootstrap'

require 'parallel'
require 'rest-client'
require 'timeout'
require 'mail'
require 'gli'
require 'openssl'
require 'ecdsa'
require 'oj' if RUBY_ENGINE == 'ruby'
require 'securerandom'
require 'http'
require 'securerandom'
require 'socksify/http'
require 'lz4-ruby'
require 'zlib'
require 'selenium-webdriver'

module RestClient
	class Request
		if instance_methods(false).include?(:net_http_object) == false
			puts "RestClient::Request does not have :net_http_object"
		elsif instance_methods(false).include?(:net_http_object_without_socksify) == false
			# puts "Monkey patch: applying socks5 support for RestClient::Request"
			def net_http_object_with_socksify(hostname, port)
				p_uri = proxy_uri
				if p_uri && p_uri.scheme =~ /^socks5?$/i
					return Net::HTTP.SOCKSProxy(p_uri.hostname, p_uri.port).new(hostname, port)
				end
				net_http_object_without_socksify(hostname, port)
			end
			alias_method :net_http_object_without_socksify, :net_http_object
			alias_method :net_http_object, :net_http_object_with_socksify
		end
	end
end

module URN
	# Common functions here
	def self.async(opt={}, &block)
		name = opt[:name] || 'unamed'
# 		puts "async #{name} invoked, thread prio #{Thread.current.priority}", level:2
		Concurrent::Future.execute(executor: URN::CachedThreadPool) {
			Thread.current.abort_on_exception = true
			Thread.current[:name] = "_async " + name
# 			puts ["async #{name} running", URN::CachedThreadPool.largest_length], level:3
			ret = block.call()
# 			puts ["async #{name} finished", URN::CachedThreadPool.largest_length], level:3
			ret
		}
	end

	def self.async_warmup(num)
		puts "Warming up for async threadpool #{num}"
		future_list = num.times.map { |i|
			URN.async(name: "warm up #{i}") {
				10000.times { JSON.parse(JSON.pretty_generate({1=>2})) }
			}
		}
		future_list.each_with_index { |f, i|
			loop {
				break if f.complete?
				sleep 0.1
			}
		}
		puts "Warming up for async threadpool #{num} finished", level:2
	end

	class FutureWatchdog
		def initialize(arg=nil, &block)
			@thread = arg if arg.is_a?(Thread)
			@block = block if block_given?
		end
		def update(time, value, reason)
			@thread.wakeup if @thread != nil
			@block.call(time, value, reason) if @block != nil
		end
	end

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

		def common_prefix(pairs)
			common_prefix_len = 0
			loop {
				break if pairs.select { |p| p.size < common_prefix_len }.size > 0
				if pairs.map { |p| p[0..(common_prefix_len)] }.uniq.size == 1
					common_prefix_len += 1
				else
					break
				end
			}
			pair_prefix = nil
			pair_prefix = pairs[0][0..(common_prefix_len-1)] if common_prefix_len > 0
			pair_prefix
		end

		# Keep sleeping even always waked up by other threads.
		def keep_sleep(seconds)
			until_t = Time.now.to_f + seconds
			loop {
				remained_t = until_t - Time.now.to_f
				break if remained_t <= 0
				sleep remained_t # Always waked up by other threads.
			}
		end
	end

	module MathUtil
		def diff(f1, f2)
			f1 = f1.to_f
			f2 = f2.to_f
			return 9999999 if [f1, f2].min <= 0
			(f1 - f2) / [f1, f2].min
		end

		def stat_array(array)
			return [0,0,0,0] if array.nil? || array.empty?
			n = array.size
			sum = array.reduce(:+)
			mean = sum/n
			deviation = array.map { |p| (p-mean)*(p-mean) }.reduce(:+)/n
			deviation = Math.sqrt(deviation)
			[n, sum, mean, deviation]
		end

    def rough_num(f)
			f ||= 0
			if f.abs > 100
        return f.round
			elsif f.abs > 1
        return f.round(2)
			elsif f.abs > 0.01
        return f.round(4)
			elsif f.abs > 0.0001
        return f.round(6)
			elsif f.abs > 0.000001
        return f.round(8)
      else
        f
      end
    end

		def format_num(f, float=8, decimal=8)
			return ''.ljust(decimal+float+1) if f.nil?
			return ' '.ljust(decimal+float+1, ' ') if f == 0
			return f.rjust(decimal+float+1) if f.is_a? String
			if float == 0
				f = f.round
				return ' '.ljust(decimal+float+1, ' ') if f == 0
				return f.to_s.rjust(decimal+float+1, ' ')
			end
			num = f.to_f
			f = "%.#{float}f" % f
			loop do
        break unless f.include?('.')
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

		def parse_contract(contract)
			segs = contract.split('@')
			asset1, asset2, expiry = nil, nil, nil
			raise "Invalid contract #{contract}" if segs.size > 2
			expiry = segs[1] if segs.size == 2
			segs = segs[0].split('-')
			raise "Invalid contract #{contract}" if segs.size > 2
			asset1, asset2 = segs
			return [asset1, asset2, expiry]
		end

		def format_order(o)
			o ||= {}
			return "#{'-'.ljust(5)}#{format_num(o['p'].to_f, 10, 4)}#{format_num(o['s'].to_f)}" if o['T'].nil?
			"#{o['T'].ljust(5)}#{format_num(o['p'].to_f, 10, 4)}#{format_num(o['s'].to_f)}"
		end

		def format_trade(t, opt={})
			return "Null trade" if t.nil?
			s = nil
			begin
				a = [
					"#{t['market']}#{t['account']}"[0..6].ljust(7),
					(t['T']||'?').ljust(4)[0].upcase,
					format_num(t['p'], 10, 3),
					format_num(t['executed'], 3, 5) + '/' + format_num(t['s'], 3, 5),
					(t['status']||'-')[0..8].ljust(9), # canceling
					(t['i']||'-').to_s.ljust(6)[-6..-1].ljust(5),
					format_trade_time(t['t'])
				]
				if t['maker_size'] != nil && t['s'] != nil
					if t['maker_size'].to_f == t['s'].to_f
						a.push('  ')
					else
						a.push('Tk'.on_white)
					end
				else
					a.push('??')
				end
				if t['v'] != nil
					# Print size for volume based order.
					a[3] = format_num(t['executed_v'], 1, 7) + '/' + format_num(t['v'], 1, 7)
					a.push("S:#{format_num(t['executed'], 4, 2)}")
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
			
			s = "#{opt[:show]}: #{t[opt[:show]]}\n#{s}" if opt[:show] != nil

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
			DateTime.strptime(t.to_s, '%Q').strftime('%H:%M:%S %y-%m-%d')
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
			# At least add a new key, to avoid error:
			# 	can't add a new key into hash during iteration
			# When set new p_real while background thread is dumping json.
			o['p_real'] = nil if o['p_real'].nil?
			if o['v'].nil?
				# 3.3966055045870003
				# 0.00033031000000960375 => round(13)
				# 764.0000000000001 => round(12)
				# 741.000000000001 => round(11)
				# 764.00000000001 => round(10)
				if o['status'] == 'pending'
					# Pending order might have no maker_size
					['s', 'executed', 'remained', 'p'].
						each { |k| o[k] = o[k].round(10) }
					if o['maker_size'] != nil
						o['maker_size'] = o['maker_size'].round(10)
					end
				else
					['s', 'executed', 'remained', 'p', 'maker_size'].
						each { |k| o[k] = o[k].round(10) }
				end
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
			if o1['market'] == 'Gemini' && o2['market'] == 'Gemini' && (o1['pair'].nil? || o2['pair'].nil?)
				; # Allow o1 / o2 without pair info, Gemini allow querying order without pair
			else
				return false if o1['pair'] != o2['pair']
			end
			if o1['T'] != nil && o2['T'] != nil
				return false if o1['T'] != o2['T']
			end
			# Some exchanges do not assign order id for instant filled orders.
			# We could identify them only by price-size-timestamp
			if o1['i'].to_s > '0' && o2['i'].to_s > '0'
				return o1['i'] == o2['i']
			elsif o1['client_oid'] != nil && o2['client_oid'] != nil
				if o1['s'] != nil && o2['s'] != nil
					return o1['s'] == o2['s'] && o1['p'] == o2['p']
				elsif o1['v'] != nil && o2['v'] != nil
					return o1['v'] == o2['v'] && o1['p'] == o2['p']
				end
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
			# Count cancelled order's executed part only in size_sum
			orders_size_sum = orders.map do |o|
				raise "error order, no size or executed\n#{o.to_json}" if o['s'].nil? || o['executed'].nil?
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
			maker_fee = o.dig('_data','fee',"maker/#{type}") || raise(JSON.pretty_generate(o))
			taker_fee = o.dig('_data','fee',"taker/#{type}") || raise(JSON.pretty_generate(o))
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
		include APD::CLI
	end

	module EmailUtil
		def email_plain(receiver, subject, content, bcc = nil, opt={})
			opt[:allow_fail] = true if opt[:allow_fail].nil? # Default allow_fail
			if opt[:skip_dup] != false
				@sent_emails ||= {}
				last_sent_t = @sent_emails["#{receiver}/#{subject}"]
				now = DateTime.now
				if last_sent_t != nil && (now - last_sent_t) < 1.0/24.0
					APD::Logger.highlight "Last t #{last_sent_t} skip sending same email:\n#{subject}\n#{(content || '')[0..99]}"
					return
				end
				@sent_emails["#{receiver}/#{subject}"] = now
			end

			content = content.gsub(" ", '&nbsp;').gsub("\n", "<br>")
			loop do
				begin
					t = Thread.new do
						APD::MailTask.email_plain(receiver, subject, content, bcc, opt)
					end
					return t
				rescue ThreadError => e
					sleep 1
					retry if (e.message || '').include?('Resource temporarily unavailable')
					return if opt[:allow_fail] == true
					raise e
				rescue
					return if opt[:allow_fail] == true
					raise e
				end
			end
		end
	end

	# Fetch trader status from redis and conf files.
	module TraderStatusUtil
		include APD::LogicControl
		include URN::OrderUtil
		include URN::Misc
		def redis
			URN::RedisPool
		end

		def latest_bulk_trader_orders(pair, from_ms)
			files = Dir["#{URN::ROOT}/bulk/*#{pair}*.json"]
			puts "#{files.size} files got for #{pair} bulk trader"
			files = files.select { |f| File.mtime(f).to_f * 1000 > from_ms }
			puts "#{files.size} files filtered for #{pair} bulk trader"
			return files.map { |f|
				puts "Loading #{File.basename(f)} orders"
				status = parse_json(File.read(f))
				next [] if status['orders'].nil?
				next [] if status['orders'].empty?
				(status['orders'] || {}).values.reduce(:+).select { |o|
					o['t'] >= from_ms || order_alive?(o)
				}
			}.reduce(:+) || []
		end

		def latest_swap_trader_orders(pair, from_ms)
			files = Dir["#{URN::ROOT}/swap/*#{pair}*.json"].to_a
			# USDT-STORJ orders might also in USD-STORJ@P swap tasks
			if pair =~ /^USDT-/
				files += Dir["#{URN::ROOT}/swap/*#{pair.gsub('USDT', 'USD')}*.json"].to_a
			elsif pair =~ /^USD-/
				files += Dir["#{URN::ROOT}/swap/*#{pair.gsub('USD', 'USDT')}*.json"].to_a
			end
			puts "#{files.size} files got for #{pair} swap trader"
			files = files.select { |f| File.mtime(f).to_f * 1000 > from_ms }
			puts "#{files.size} files filtered for #{pair} swap trader"
			return files.map { |f|
				puts "Loading #{File.basename(f)} orders"
				status = parse_json(File.read(f))
				maker_orders = (status['orders'] || {}).values.reduce(:+)
				maker_orders = (maker_orders || []).select { |o|
					o['t'] >= from_ms || order_alive?(o)
				}
				legacy_orders = []
				if status.dig('task', 'legacy_info') != nil
					legacy_orders = status['task']['legacy_info'].values.reduce(:+)
					legacy_orders = (legacy_orders || []).select { |o|
						o['t'] >= from_ms || order_alive?(o)
					}
				end
				maker_orders + legacy_orders
			}.reduce(:+) || []
		end

		def all_swap_hedge_files()
			[
				Dir["#{URN::ROOT}/swap/hedge_*.json"],
				Dir["#{URN::ROOT}/swap/open_*.json"],
				Dir["#{URN::ROOT}/swap/reduce_*.json"],
				Dir["#{URN::ROOT}/swap/close_*.json"]
			].map { |l| l.to_a }.reduce(:+)
		end

		def all_swap_files()
			[
				Dir["#{URN::ROOT}/swap/swap_*.json"],
				Dir["#{URN::ROOT}/swap/hedge_*.json"],
				Dir["#{URN::ROOT}/swap/open_*.json"],
				Dir["#{URN::ROOT}/swap/reduce_*.json"],
				Dir["#{URN::ROOT}/swap/close_*.json"]
			].map { |l| l.to_a }.reduce(:+)
		end

		def trader_status(pair)
			pair = pair.upcase
			keys = redis.keys "URANUS:orders:#{pair}*"
			if keys.empty?
				puts "No redis keys for URANUS:orders:#{pair}*".red
				return {}
			end
			raise "More than one key for pair #{pair}: #{keys}" unless keys.size == 1
			key = keys.first
			fields = redis.hkeys(key)
			raise "More than one field for key #{key}" unless fields.size == 1
			f = fields.first
			raise "field not start with abt_union: #{f}" unless f.start_with?('abt_union')
			data = redis.hget key, f
			data = JSON.parse data
		end

		def trader_full_state_on_disk(pair, opt={})
			verbose = opt[:verbose] == true
			content = nil
			dir=nil
			file=nil

			if pair =~ /^[0-9A-Za-z\-]{1,12}$/
				if ENV['URANUS_RAMDISK'] != nil
					dir = "#{ENV['URANUS_RAMDISK']}/trader_state/"
					file = Dir["#{dir}/*"].
						select { |f| f.upcase.include?(pair.upcase) }.first
				end
				if file.nil?
					dir = "./trader_state/"
					file = Dir["#{dir}/*"].
						select { |f| f.upcase.include?(pair.upcase) }.first
				end
				puts pair if verbose
				puts dir if verbose
				puts file if verbose
				raise "No match file #{file} #{pair}" if file.nil?
			end

			if file.end_with?('.lz4')
				content = LZ4::decompress(File.read(file))
			elsif file.end_with?('.gz')
				Zlib::GzipReader.open(file) { |gz| content = gz.read }
			elsif file.end_with?('.json')
				content = File.read(file)
			elsif file.include?('.gz.') # history state
				Zlib::GzipReader.open(file) { |gz| content = gz.read }
			else
				content = File.read(file)
			end
			return Oj.load(content)
		end

		# Scan bin/tmux_uranus* for *active* managed arbi tasks.
		def available_trading_pairs()
			pairs = []
			Dir["#{URN::ROOT}/bin/tmux_uranus*.sh"].each { |f|
				File.read(f).split("\n").each { |l|
					next if l.strip.empty?
					l = l.strip.split('#')[0] # remove comment
					next unless l.include?('/task/arbitrage_')
					pair = l.split('/task/arbitrage_')[1].split('.sh')[0]
					if pair.include?('_')
						pair = pair.split('_').join('-').upcase
					else
						pair = "BTC-#{pair}".upcase
					end
					pairs.push(pair)
				}
			}
			pairs
		end
		# Return pair list in /task/arbitrage_*
		def available_trading_pairs_in_task_dir()
			pairs = Dir["#{URN::ROOT}/task/arbitrage_*.sh"].map { |f|
				p = File.basename(f).split('arbitrage_')[1].split('.sh')[0].upcase.gsub('_', '-')
				p = "BTC-#{p}" unless p.include?('-')
				p
			}
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
			if base == 'BTC'
				file = "#{URN::ROOT}/task/arbitrage_#{asset.downcase}.sh"
			else
				file = "#{URN::ROOT}/task/arbitrage_#{base.downcase}_#{asset.downcase}.sh"
			end
			return [] unless File.file?(file)
			File.read(file).
				split("\n").select { |l| l.include?('arbitrage_base.sh') }.
				select { |l| l.start_with?('#') == false }.first.
				split(" ")[4..-1].
				map { |m| m.gsub("'",'').split('@').first }
		end

		def active_spider_tasks
			file = "#{URN::ROOT}/bin/tmux_uranus.sh"
			lines = endless_retry(sleep: 5) {
				File.read(file).split("\n").select { |l| l.include?('/spider.sh') }
			}
			result = {}
			lines.each { |l|
				args = l.split('spider.sh')[1].gsub('"', '').strip.upcase.split(' ')
				market = args.shift
				market = 'FTX' if market =~ /^FTX[0-9]*/ # FTX0-9 sub tasks -> FTX
				market = 'BYBITU' if market =~ /^BYBITU[0-9]*/ # BYBITU0-9 sub tasks -> FTX
				pairs = args.map { |a| (a.include?('-') ? a : "BTC-#{a}") }
				pairs.each { |p|
					result[p] ||= {}
					result[p][market] = true
					quote, asset = p.split('-')
					if market == 'GEMINI'
						if quote == 'USD'
							# Gemini also publish usd pairs data to usdt pairs
							alt_p = p.gsub('USD-', 'USDT-')
							result[alt_p] ||= {}
							result[alt_p][market] = true
						end
					elsif market == 'COINBASE'
						if ['USDC-ADA', 'USDC-BAT', 'USDC-CVC', 'USDC-DNT', 'USDC-MANA', 'USDC-ZEC'].include?(p)
							# Coinbase re-direct these pairs to USDT pairs
							alt_p = p.gsub('USDC-', 'USDT-')
							result[alt_p] ||= {}
							result[alt_p][market] = true
						end
						if p == 'USDC'
							# Coinbase uses USD pair as USDC
							alt_p = p.gsub('USDC-', 'USD-')
							result[alt_p] ||= {}
							result[alt_p][market] = true
							result[p].delete(market)
						end
					elsif market == 'BINANCE'
						if quote == 'BUSD'
							# Binance publish busd pairs data to usd pairs
							alt_p = p.gsub('BUSD-', 'USD-')
							result[alt_p] ||= {}
							result[alt_p][market] = true
							result[p].delete(market)
						end
					elsif market == 'FTX'
						if ['USD-CVC', 'USD-STORJ'].include?(p)
							# FTX re-direct these pairs to USDT pairs
							alt_p = p.gsub('USD-', 'USDT-')
							result[alt_p] ||= {}
							result[alt_p][market] = true
						end
					end
				}
			}
			result
		end
	end

	module CoingeckoUtil
		include APD::LogicControl
		include APD::SpiderUtil
		include APD::ExpireResult

		COINGECKO_CACHE_T ||= 300

		def coingecko_stat(opt={})
			list = coingecko_list(opt)
			btc_price = list.dig('BTC', 'price')
			raise "No BTC price." if btc_price.nil?
			map = {}
			# Merge and sort by market cap, evaluate rank again.
			usd_mc_ttl, btc_mc_ttl, eth_mc_ttl, alt_mc_ttl = 0, 0, 0, 0
			list.values.
				sort_by { |d| d['market_cap'] }.
				reverse.each_with_index { |d, i|
					d['price_btc'] = d['price'].to_f/btc_price
					d['price_sat'] = (d['price_btc'].to_f * 100000000).to_i
					d['24h_volume_usd'] = d.delete('volume')
					d['24h_volume_btc'] = (d['24h_volume_usd'].to_f/btc_price).to_i
					d['rank'] = i+1
					case d['name']
					when 'BTC'
						btc_mc_ttl += d['market_cap']
					when 'ETH'
						eth_mc_ttl += d['market_cap']
					when /(UST|DAI|USD|PAXG|XAUT)/ # Any asset contains these.
						usd_mc_ttl += d['market_cap']
					when /(BTC|ETH|CRV)/ # Any asset contains BTC/ETH : aWBTC, cETH, CRVLP
						;
					else
						alt_mc_ttl += d['market_cap']
					end
					map[d['name']] = d
			}
			@_coingecko_mc_stat = {
				:usd_mc_ttl => usd_mc_ttl,
				:btc_mc_ttl => btc_mc_ttl,
				:eth_mc_ttl => eth_mc_ttl,
				:alt_mc_ttl => alt_mc_ttl
			}
			map
		end

		def coingecko_mc_stat
			coingecko_stat() if @_coingecko_mc_stat.nil?
			return @_coingecko_mc_stat
		end
	
		def coingecko_list(opt={})
			coin_list = {}
			(1..5).each do |i|
				url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page=#{i}"
				coingecko_api_list(url, coin_list)
			end
			coin_list
		end
		expire_every COINGECKO_CACHE_T, :coingecko_list

		def coingecko_api_list(url, coin_list, opt={})
			response = endless_retry(sleep:5) { RestClient.get(url, accpet: :json) }
			JSON.parse(response).each { |r|
				name = r['symbol'] || raise("No symbol in #{r}")
				name = name.upcase
				full_name = r['name'] || raise("No name in #{r}")
				price = r['current_price'] # || raise("No current_price in #{r}")
				next if price.nil?
				rank = r['market_cap_rank'] || 99999
				percent_change_24h = r['price_change_percentage_24h'] || 0
				market_cap = r['market_cap'] || raise("No market_cap in #{r}")
				volume = r['total_volume'] || raise("No total_volume in #{r}")

				name = 'IOTA' if name == 'MIOTA'
				# Only accept ATOM:Cosmos BTT:BitTorrent
				next if name == 'ATOM' && full_name.include?('Cosmos')==false
				next if name == 'BTT' && full_name.include?('BitTorrent') == false

				if coin_list[name] != nil
					# puts "Duplicate coin name #{coin_list[name]['full_name']} - #{cols[1]}"
				elsif name =~ /^[A-Z0-9]{1,9}$/
					coin_list[name] = {
						'rank'	=> rank,
						'name'	=> name,
						'full_name'	=> full_name,
						'market_cap'	=> market_cap,
						'price'	=> price,
						'volume'	=> volume,
						'percent_change_24h'	=> percent_change_24h
					}
				else
					# puts "Unknown coin name #{name.inspect}"
				end
			}
		end
	end

	module CoinMarketUtil # Web parser outdated.
		include APD::LogicControl
		include APD::SpiderUtil
		include APD::ExpireResult

		COIN_MARKET_CACHE_T ||= 300

		def coinmarket_stat_top99(opt={})
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
		expire_every COIN_MARKET_CACHE_T, :coinmarket_stat_top99

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
			html = endless_retry(sleep:5) { parse_html(render_html(url, with: :firefox, render_t: 1)) }
			# html = endless_retry(sleep:5) { parse_web(url, verbose:true, max_time: 10) }
			# html = endless_retry(sleep:5) { parse_html(render_html(url, with: :phantomjs)) }
			html.clone.xpath("//table/tbody/tr").each do |line|
				el_list = line.children.select { |c| c.name == 'td' }
				cols = el_list.map { |c| c.text.strip }
				cols.each_with_index { |c, i| puts "#{i} [#{c}]" }
				# 10/05-10:45:18.5068 .otstrap:649 0 []
				# 10/05-10:45:18.5069 .otstrap:649 1 [1]
				# 10/05-10:45:18.5069 .otstrap:649 2 [Tether1USDT]
				# 10/05-10:45:18.5069 .otstrap:649 3 [$1.000.02%]
				# 10/05-10:45:18.5070 .otstrap:649 4 [0.02%]
				# 10/05-10:45:18.5070 .otstrap:649 5 [0.02%]
				# 10/05-10:45:18.5070 .otstrap:649 6 [$15,633,962,967]
				# 10/05-10:45:18.5070 .otstrap:649 7 [$31,344,598,03031,298,401,458 USDT]
				# 10/05-10:45:18.5070 .otstrap:649 8 [15,610,921,182 USDT]
				# 10/05-10:45:18.5071 .otstrap:649 9 []
				# 10/05-10:45:18.5071 .otstrap:649 10 []
				next if cols.nil? || cols.size < 9
				next if cols[7].empty? # Empty line

				rank = cols[1].to_i
				name_str = cols[2].split(rank.to_s)
				full_name = name_str[0]
				name = name_str[1]
				price_el = el_list[3]
				if price_el.nil? # TODO
					puts "Skip #{name} #{full_name}, no price element"
					next
				end
				price = price_el.text.strip.gsub('$','').gsub(',','').to_f
				percent_change_24h = cols[4].gsub('$','').gsub(',','').gsub('*','').to_f
				percent_change_7d = cols[5].gsub('$','').gsub(',','').gsub('*','').to_f
				market_cap = cols[6].gsub('$','').gsub(',','').gsub('*','').to_i
				volume = el_list[7].
					children.find { |c| c.name == 'div' }.
					children.find { |c| c.name == 'a' }.
					text.strip.gsub('$','').gsub(',','').to_i

				name = 'IOTA' if name == 'MIOTA'
				# Only accept ATOM:Cosmos BTT:BitTorrent
				next if name == 'ATOM' && full_name.include?('Cosmos')==false
				next if name == 'BTT' && full_name.include?('BitTorrent') == false

				if coin_list[name] != nil
					# puts "Duplicate coin name #{coin_list[name]['full_name']} - #{cols[1]}"
				elsif name =~ /^[A-Z0-9]{1,9}$/
					coin_list[name] = {
						'type'	=> type,
						'rank'	=> rank,
						'name'	=> name,
						'full_name'	=> full_name,
						'market_cap'	=> market_cap,
						'price'	=> price,
						'volume'	=> volume,
						'percent_change_24h'	=> percent_change_24h
					}
				else
					# puts "Unknown coin name #{name.inspect}"
				end
			end
		end
	
		def coinmarket_coin_list(opt={})
			coin_list = {}
			(1..5).each do |i|
				url = "https://coinmarketcap.com/coins/#{i}/"
				_coinmarket_parse_list(url, coin_list, type:'coin')
			end
			coin_list
		end
		expire_every COIN_MARKET_CACHE_T, :coinmarket_coin_list
	
		def coinmarket_token_list(opt={})
			token_list = {}
			(1..5).each do |i|
				url = "https://coinmarketcap.com/tokens/#{i}/"
				_coinmarket_parse_list(url, token_list, type:'token')
			end
			token_list
		end
		expire_every COIN_MARKET_CACHE_T, :coinmarket_token_list
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
	URN::ROOT = File.expand_path(File.dirname(__FILE__) + '/../')
	URN::API_PROXY = (ENV['API_PROXY'] || 'default').
		split(',').
		map { |str| str=='default'?nil:str }
	# Global resources.
	# will create as many threads as necessary for work posted to it
	# https://ruby-concurrency.github.io/concurrent-ruby/master/Concurrent/ThreadPoolExecutor.html
	URN::CachedThreadPool = Concurrent::CachedThreadPool.new(idletime:2147483647)
	# Set available redis pool num to zero, so it would not always connect even not needed.
	URN::RedisPool = APD::TransparentGreedyPoolProxy.new(
		APD::GreedyRedisPool.new(0, redis_db:0, warn_time:0.005, warn_stack:6)
	)
	# Example to suppress most redis pool warning.
	# URN::RedisPool.pool.warn_time = 1.0
	URN::BOOTSTRAP_LOAD = true
end
