require_relative '../common/bootstrap'

class MarketDataFlusher
	include APD::CacheUtil

	def redis_db
		0
	end

	def initialize
		@channel_map = {}
		@f_writer = {}
		@files = {}
		@f_writer_ct = {}
		@data_dir = [URN::ROOT, 'data', 'subscribe'].join('/')
		FileUtils.mkdir_p @data_dir
	end

	def find_data_key(channel)
		k = @channel_map[channel]
		return k unless k.nil?
		k = channel.split('_channel').first
		@channel_map[channel] = k
		k
	end

	# Multiple types would share one writer.
	def find_writer(prefix, market, pair)
		key = "#{prefix}:#{market}:#{pair}"
		w = @f_writer[key]
		return w unless w.nil?

		@_init_writer = true
		time = DateTime.now.strftime('%Y%m%d_%H%M%S')
		file = "#{market}_#{pair}.#{time}.txt.gz"
		@files[key] = file
		puts "Open new file #{file} for #{key}"
		writer = Zlib::GzipWriter.open("#{@data_dir}/#{file}")
		@f_writer[key] = writer
		@_init_writer = false
		writer
	end

	def write_msg(channel, msg)
		prefix, market, pair, type = channel.split(':')
		# Multiple types share one writer.
		writer = find_writer(prefix, market, pair)
		intro = type
		if type == 'full_odbk_channel'
			intro = 'odbk'
		elsif type == 'full_tick_channel'
			intro = 't'
		end
		writer.puts(intro)
		writer.puts(msg)

		@f_writer_ct[channel] ||= 0
		@f_writer_ct[channel] += 1
		if @f_writer_ct[channel] % 100 == 0
			key = "#{prefix}:#{market}:#{pair}"
			puts "Flush #{@files[key]}"
			writer.flush
		end
	end

	def on_message(channel, msg)
		data = nil
		if msg == '1'
			raise "this kind of updates does suit for HFT"
			data_key = find_data_key(channel)
			data = redis.get data_key
		else
			data = msg # Broadcast data directly
		end
		# puts [data_key, data.size] # make it faster
		write_msg(channel, data)
	end

	def work(markets)
		redis() # Init default redis client.
		channels = markets.map { |m| "URANUS:#{m}:*:full_odbk_channel" }
		channels += markets.map { |m| "URANUS:#{m}:*:full_tick_channel" }
		puts channels
		# Use a new redis client to subscribe
		# while internal one is used to read data.
		redis_new.psubscribe(*channels) do |on|
			on.psubscribe do |channel, subscriptions|
				puts "Subscribed to ##{channel} (#{subscriptions} subscriptions)"
			end
			on.pmessage do |pattern, channel, msg|
				on_message(channel, msg)
			end
			on.punsubscribe do |channel, subscriptions|
				puts "Unsubscribed to ##{channel} (#{subscriptions} subscriptions)"
			end
		end
	end
end

markets = ARGV
raise "No target market!" if markets.empty?
MarketDataFlusher.new.work(markets)
