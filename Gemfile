source 'https://rubygems.org'

# Install gems from Aphrodite dir.
gemfile = "#{File.dirname(__FILE__)}/../aphrodite/Gemfile"
eval(IO.read(gemfile), binding)

gem 'parallel'
gem 'gli'
gem 'ecdsa'
gem 'oj' if RUBY_ENGINE == 'ruby'
gem 'socksify'
gem 'lz4-ruby'
gem 'http', '5.0.0.pre3'

gem 'sinatra'
gem 'sinatra-namespace'

# Install gems from Gemfiles dir.
Dir["#{File.dirname(__FILE__)}/Gemfiles/*"].each do |gemfile|
	eval(IO.read(gemfile), binding)
end
