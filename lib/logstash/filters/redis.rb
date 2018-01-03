# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'redis'
require "logstash/json"
require "lru_redux"

class LogStash::Filters::Redis < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   redis {
  #     host => "127.0.0.1"
  #     port => 6379
  #     db => 0
  #     # password => ""
  #     action => "GET"
  #     key => "%{host}"
  #     field => "%{host}"
  #     name => "ext"
  #     default => ""
  #   }
  # }
  #
  config_name "redis"

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Connection timeout
  config :timeout, :validate => :number, :default => 5

  # Interval for reconnecting to failed Redis connections
  config :reconnect_interval, :validate => :number, :default => 1

  # The action of a redis filter
  config :action, :validate => ["GET", "SMEMBERS", "HGET", "ZSCORE"], :required => true, :default => "GET"

  # The name of a redis key
  config :key, :validate => :string, :required => true

  # The name of a redis key
  config :member, :validate => :string, :require => false
  config :field, :validate => :string, :require => false
  config :default, :validate => :string, :require => false

  config :ttl, :validate => :number, :default => 60.0
  config :lru_cache_size, :validate => :number, :default => 10000

  attr_accessor :lookup_cache

  public
  def register
    @redis = nil
    @logger.debug("Registering Redis Filter plugin")
    self.lookup_cache ||= LruRedux::TTL::ThreadSafeCache.new(@lru_cache_size, @ttl)
    @logger.debug("Created cache...")
  end

  # def register

  public
  def filter(event)
    name = event.sprintf(@name)
    value = @default
    cached_value = lookup_cache[name]
    if (cached_value.nil?)
      begin
        key = event.sprintf(@key)
        value = nil
        @redis ||= connect
        case @action
          when "ZSCORE"
            member = event.sprintf(@member)
            value = @redis.zscore(key, member)
          when "GET"
            value = @redis.get(key)
          when "SMEMBERS"
            value = @redis.smembers(key)
          when "HGET"
            field = event.sprintf(@field)
            value = @redis.hget(key, field)
        end
      rescue => e
        @logger.warn("Failed to get value from redis", :event => event,
                     :identity => identity, :exception => e, :action => @action, :backtrace => e.backtrace)
        sleep @reconnect_interval
        @redis = nil
        retry
      end
    end
    event.set(name, value)
    lookup_cache[name] = value

    return filter_matched(event)
  end

  # def filter

  # A string used to identify a Redis instance in log messages
  def identity
    "redis://#{@password}@#{@host}:#{@port}/#{@db} #{@action} #{@key}"
  end

  private
  def connect
    params = {
        :host => @host,
        :port => @port,
        :timeout => @timeout,
        :db => @db,
    }
    @logger.debug("connection params", params)

    if @password
      params[:password] = @password.value
    end

    Redis.new(params)
  end
end # class LogStash::Filters::Redis