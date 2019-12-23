module URN
	# Receive and cache market order updates
	# Push updates to listener or just provide in-mem query.
	# If listener is a Thread, wakeup() would be invoked.
	# Otherwise listener.on_oms_broadcast(mkt, i, json_str) is called.
	class OMSLocalCache
		@@_oms_broadcast_channels ||= {}
		@@oms_local_cache ||= Concurrent::Hash.new
		@@ready_markets ||= Concurrent::Hash.new
		@@listeners ||= Concurrent::Array.new

		def self.redis
			URN::RedisPool
		end

		def self.monitor(markets, listeners=[])
			listeners.each { |l| add_listener(l) }

			channels = []
			started_markets = markets.select do |mkt|
				if @@oms_local_cache[mkt] != nil
					puts "OMSLocalCache is running for ##{mkt} already, skip".red
					next false
				end
				@@oms_local_cache[mkt] = Concurrent::Hash.new
				# oms.js : `URANUS:${exchange}:${account}:O_channel`;
				@@_oms_broadcast_channels["URANUS:#{mkt}:-:O_channel"] = mkt
				channels.push "URANUS:#{mkt}:-:O_channel"
				next true
			end
			
			if started_markets.size > 0
				t = Thread.new {
					begin
						_listen_oms(channels)
					rescue => e
						APD::Logger.error e
					end
				}
				t.priority = 2
				puts "OMS cache started for #{started_markets}"
			end
		end

		def self.add_listener(l)
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
			@@ready_markets[market] == true
		end

		def self.oms_info(mkt, id)
			@@oms_local_cache.dig(mkt, id)
		end

		def self._listen_oms(channels)
			puts "<< OMS cache subscribing #{channels}"
			redis.subscribe(*channels) do |on|
				on.subscribe do |chn, num|
					puts "<< OMS cache subscribed to #{chn} (#{num} subscriptions)"
					# Get full snapshot of orders
					mkt = @@_oms_broadcast_channels[chn] # {id:json_str}
					prefix = "URANUS:#{mkt}:-:O:"
					hash_names = redis.keys('URANUS*').select { |n| n.start_with?(prefix) }
					@@oms_local_cache[mkt].clear
					hash_names.each do |hash_name|
						order_map = redis.hgetall(hash_name)
						t = order_map.delete('t')
						if t.nil?
							puts "<< OMS cache #{mkt} no valid t for #{hash_name}"
						else
							@@oms_local_cache[mkt].merge!(order_map)
							puts "<< OMS cache #{mkt} init with #{hash_name} #{order_map.size} orders"
							@@ready_markets[mkt] = true
						end
					end
				end
				on.message do |chn, msg|
					begin
						mkt = @@_oms_broadcast_channels[chn] # {id:json_str}
						if msg == 'CLEAR' # Execute remote signal
							puts "<< OMS cache #{mkt} signal: CLEAR".red
							@@ready_markets[mkt] = false
							@@oms_local_cache[mkt].clear
							next
						end
						msg = JSON.parse(msg)
						puts "<< OMS cache #{mkt} #{msg.keys}"
						@@oms_local_cache[mkt].merge!(msg)
						@@listeners.each { |l|
							if l.is_a?(Thread)
								l.wakeup
							else
								msg.each { |id, json| l.on_oms_broadcast(mkt, id, json) }
							end
						}
					rescue => e
						APD::Logger.error e
					end
				end
				on.unsubscribe { |chn, num| raise "Unsubscribed to ##{chn} (#{num} subscriptions)" }
			end
		end
	end
end
