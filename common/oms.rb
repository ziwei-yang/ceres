module URN
	# Receive and cache market order updates
	# Push updates to listener or just provide in-mem query.
	# If listener is a Thread, wakeup() would be invoked.
	# Otherwise listener.on_oms_broadcast(mkt, i, json_str) is called.
	class OMSLocalCache
		class << self
			include APD::LogicControl
			include URN::OrderUtil
			include URN::Misc
		end

		@@_oms_broadcast_channels ||= {}
		@@oms_local_cache ||= Concurrent::Hash.new
		# Receiving data for market
		@@work_markets ||= Concurrent::Hash.new
		# Full data snapshot is ready for market
		@@inited_markets ||= Concurrent::Hash.new
		@@listeners ||= Concurrent::Array.new
		@@max_cache_size ||= Concurrent::Hash.new
		@@current_cache_size ||= Concurrent::Hash.new
		@@verbose = true

		def self.redis
			URN::RedisPool
		end

		def self.log(*args)
			puts *args if @@verbose != false
		end

		def self.monitor(market_account_map, listeners=[], opt={})
      if market_account_map.is_a?(Array)
        markets = market_account_map
        market_account_map = {}
      else
        markets = market_account_map.keys.uniq
      end
      # Rebuild map with default accounts.
      @@oms_account = market_account_map.clone
      markets.each { |m| @@oms_account[m] ||= '-' }
			@@verbose = opt[:verbose] == true
			listeners.each { |l| add_listener(l) }

			channels = []
			started_markets = markets.select do |mkt|
				if @@oms_local_cache[mkt] != nil
					log "OMSLocalCache is running for ##{mkt} already, skip".red
					next false
				end
				@@oms_local_cache[mkt] = Concurrent::Hash.new
				# oms.js : `URANUS:${exchange}:${account}:O_channel`;
				@@_oms_broadcast_channels["URANUS:#{mkt}:#{@@oms_account[mkt]}:O_channel"] = mkt
				channels.push "URANUS:#{mkt}:#{@@oms_account[mkt]}:O_channel"
				next true
			end
      puts "market_account_map #{market_account_map}"
      puts "started_markets #{started_markets}"
			
			listen_thread = nil
			if started_markets.size > 0
				listen_thread = Thread.new(abort_on_exception:true) {
					Thread.current[:name] = "OMSLocalCache.listen #{started_markets}"
          puts "OMSLocalCache.listen #{started_markets} #{JSON.pretty_generate(channels)}"
					begin
						_listen_oms(channels, pair_prefix: opt[:pair_prefix])
					rescue => e
						APD::Logger.error e
					end
				}
				listen_thread.priority = 3
				log "OMS cache started for #{started_markets}"
			end

			max_wait_time = nil
			max_wait_time = 20 if opt[:wait] == true
			max_wait_time = opt[:wait] if opt[:wait].is_a?(Integer)
			if max_wait_time != nil
				start_wait_t = Time.now.to_f
				markets.each { |m|
					loop {
            break if ['Bitstamp'].include?(m)
            # IB OMS always init only when first new orders found,
            # wait for this to place new order is not practical.
            break if URN::IB_MARKETS.include?(m)
						break if URN::OMSLocalCache.support_mkt?(m)
						wait_t = Time.now.to_f - start_wait_t
						if wait_t > max_wait_time
							puts "Wait for #{m} OMS started work:#{@@work_markets} init:#{@@inited_markets}, timeout".red
							break
						end
						puts "Wait for #{m} OMS started work:#{@@work_markets} init:#{@@inited_markets}"
						sleep 3
					}
				}
			else
				raise "Please check the code, wait is not true might make listening thread has no chance to start"
			end

			return listen_thread
		end

		def self.add_listener(l)
			return if @@listeners.include?(l)
			puts "OMSLocalCache listener added #{l.class.name}"
			@@listeners.push l
		end

		def self.remove_listener(l)
			ret = @@listeners.delete l
			if ret.nil?
				puts "OMSLocalCache listener removed failed, #{l.class.name} not found".red
			else
				puts "OMSLocalCache listener removed #{l.class.name}"
			end
		end

		def self.support_mkt?(market)
			@@work_markets[market] == true && @@inited_markets[market] == true
		end

		def self.oms_info(mkt, id)
			@@oms_local_cache.dig(mkt, id)
		end

		# For mkt oms_order_write_if_null()
		def self.oms_set_if_null(mkt, id, str)
			if @@work_markets[mkt] == true
				if @@oms_local_cache.dig(mkt, id).nil?
					@@oms_local_cache[mkt][id] = str
					log ">> write to OMSCache/#{mkt} #{id}".blue
				end
			end
		end

		# For mkt oms_order_delete()
		def self.oms_delete(mkt, id)
			cache = @@oms_local_cache[mkt]
      if cache != nil
				cache.delete(id)
				log ">> delete OMSCache/#{mkt} #{id}".red
			end
		end

		# Could be run without start listening broadcast.
		# Clear dead order with age older than 1 hour.
		# Has bug with Future markets.
		def self.clear_old_dead_orders
			hash_names = endless_retry(sleep:1) {
				redis.keys('URANUS*').select { |n| n =~ /^URANUS:[a-zA-Z].*:.*:O:/ }
			}
			clients = {}
			hash_names.sort.each { |hash_name|
				market_name = hash_name.split(':')[1]
				if clients[market_name].nil?
					next if URN.const_defined?(market_name) == false
					puts "init #{market_name}"
					clients[market_name] = URN.const_get(market_name).
						new(verbose:false, skip_balance:true, trade_mode:'no')
				end
			}
			keep_sleep 5
			puts "Start cleaning old dead order from OMS cache"
			keep_sleep 5
			hash_names.sort.each { |hash_name|
				market_name = hash_name.split(':')[1]
				client = clients[market_name]
				next if client.nil?
				pair = hash_name.split(':')[4]
				order_map = endless_retry(sleep:1) { redis.hgetall(hash_name) }
				ct = 0
				hdel_args = [hash_name]
				order_map.each { |id, json|
					next if id == 't'
					json = JSON.parse(json)
					begin
						o = client.send(:_normalize_trade, pair, json)
						if order_alive?(o) == false && order_age(o) > 3600*1000
							ct += 1
							hdel_args.push(id)
							break if ct >= 99999 # Too long args would break stack.
						end
					rescue => e
						APD::Logger.highlight "Error in processing #{hash_name} #{id}"
					end
				}
				next if ct <= 0
				puts "#{market_name} #{pair} : Deleting #{ct} tuples from #{order_map.size}"
				endless_retry { redis.hdel(*hdel_args) }
			}
		end

		def self._init_order_cache(chn, opt={})
			mkt = @@_oms_broadcast_channels[chn] # {id:json_str}
			oms_running = endless_retry(sleep:1) { redis.get("URANUS:#{mkt}:#{@@oms_account[mkt]}:OMS") }
			if oms_running.nil?
				log "<< OMS #{mkt} OFF, skip _init_order_cache()".red
				return
			end

			log "<< OMS cache #{mkt} init started #{opt}"
			# Only care about pairs with those preix:
			# Example: prefix USD-BTC -> USD-BTC, USD-BTC@20200626
			pair_prefix = opt[:pair_prefix] || ''
			prefix = "URANUS:#{mkt}:#{@@oms_account[mkt]}:O:#{pair_prefix}"
			hash_names = endless_retry(sleep:1) {
				redis.keys('URANUS*').select { |n| n.start_with?(prefix) }
			}
			@@oms_local_cache[mkt].clear
			pair_ct, order_ct, order_ct_map = 0, 0, {}
			hash_names.each do |hash_name|
				order_map = endless_retry(sleep:1) { 
					log "redis.hgetall #{hash_name}"
					redis.hgetall(hash_name)
				}
				t = order_map.delete('t')
				if t.nil?
					log "<< OMS cache #{mkt} no valid t for #{hash_name}, OMS ON, treat as active"
				end
				@@oms_local_cache[mkt].merge!(order_map)
				@@work_markets[mkt] = true
				pair_ct += 1
				order_ct += order_map.size
				order_ct_map[hash_name] = order_map.size
			end
			log "<< OMS cache #{mkt} init with #{pair_ct} pairs, #{order_ct} orders\n#{JSON.pretty_generate(order_ct_map)}"
			@@inited_markets[mkt] = true
			@@work_markets[mkt] = true
			@@max_cache_size[mkt] = [2048, order_ct * 2].max
			@@current_cache_size[mkt] = order_ct
		end

		def self._listen_oms(channels, opt={})
			pair_prefix = opt[:pair_prefix] || ''
			channels.each { |chn|
				mkt = @@_oms_broadcast_channels[chn]
				@@work_markets[mkt] = false
				@@inited_markets[mkt] = false
			}
      channels.each { |chn| _init_order_cache(chn, pair_prefix: pair_prefix) }
			log "<< OMS cache subscribing #{channels}"
			redis.subscribe(*channels) do |on|
				on.subscribe do |chn, num|
					log "<< OMS cache subscribed to #{chn} (#{num} subscriptions)"
					mkt = @@_oms_broadcast_channels[chn]
					@@work_markets[mkt] = true
					# Get full snapshot of orders
					mkt = @@_oms_broadcast_channels[chn]
					_init_order_cache(chn, pair_prefix: pair_prefix) if @@work_markets[mkt] != true
				end
				on.message do |chn, msg|
					begin
						mkt = @@_oms_broadcast_channels[chn]
						if msg =~ /^SIGNAL/ # msg = {id:json_str} OR SIGNAL/COMMAND
							log "<< #{msg} - Signal received"
							if msg == 'SIGNAL/CLEAR' # Execute remote signal
								log "<< OMS cache #{mkt} signal: CLEAR".red
								@@work_markets[mkt] = false
								@@inited_markets[mkt] = false
								@@oms_local_cache[mkt].clear
							elsif msg == 'SIGNAL/ONLINE' # Execute remote signal
								_init_order_cache(chn, pair_prefix: pair_prefix) if @@work_markets[mkt] != true
							else
								log "<< #{msg} - unknown channel signal"
							end
							next
						end

						# Cache would be enabled again.
						_init_order_cache(chn, pair_prefix: pair_prefix) if @@work_markets[mkt] != true

						msg = JSON.parse(msg)
						# log "<< OMS cache #{mkt} #{msg.keys}" if @@verbose
						size = @@current_cache_size[mkt] || 0
						msg.each { |k, v| size += 1 unless @@oms_local_cache[mkt].key?(k) }
						@@oms_local_cache[mkt].merge!(msg)
						@@listeners.each { |l|
							if l.is_a?(Thread)
								l.wakeup
							else
								msg.each { |id, json| l.on_oms_broadcast(mkt, id, json) }
							end
						}

						# Maintain @@oms_local_cache : just purge when size is too big.
						max_size = @@max_cache_size[mkt]
						if max_size != nil && size >= max_size
							log "<< OMS cache #{mkt} size #{size} >= #{max_size} PURGE triggered".red
							@@work_markets[mkt] = false
							@@inited_markets[mkt] = false
							@@oms_local_cache[mkt].clear
							_init_order_cache(chn, pair_prefix: pair_prefix)
						else
							@@current_cache_size[mkt] = size
						end
					rescue => e
						APD::Logger.error e
					end
				end
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			end
		end
	end
end
