source 'https://rubygems.org'

# Install gems from Aphrodite dir.
gemfile = "#{File.dirname(__FILE__)}/../aphrodite/Gemfile"
eval(IO.read(gemfile), binding)

gem 'concurrent-ruby', require: 'concurrent'
# Potential performance improvements may be achieved under MRI 
# by installing optional C extensions.
gem 'concurrent-ruby-ext' if RUBY_ENGINE == 'ruby'

gem 'parallel'
gem 'gli'
gem 'ecdsa'
gem 'oj' if RUBY_ENGINE == 'ruby'

gem 'sinatra'
gem 'sinatra-namespace'

# Install gems from Gemfiles dir.
Dir["#{File.dirname(__FILE__)}/Gemfiles/*"].each do |gemfile|
	eval(IO.read(gemfile), binding)
end
