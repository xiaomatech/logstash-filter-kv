# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'redis'
require "lru_redux"
require "tempfile"
require "thread"

class LogStash::Filters::Tag < LogStash::Filters::Base

  config_name "tag"

  default :codec, "json"

  config :host, :validate => :string, :default => "127.0.0.1"

  config :port, :validate => :number, :default => 6379

  config :db, :validate => :number, :default => 0

  config :timeout, :validate => :number, :default => 5

  config :password, :validate => :password

  config :lru_cache_size, :validate => :number, :default => 10000

  config :prefix, :validate => :string, :required => true

  config :source, :validate => :string, :required => true

  config :target, :validate => :string, :required => true

  config :default_value, :validate => :string, :default => '{}'

  config :ttl, :validate => :number, :default => 60.0

  @@cache_storage = {}

  def new_redis_instance
    @redis_builder.call
  end

  def lookupCacheStorage(cache_key)
    if !@@cache_storage.has_key?(cache_key)
      @@cache_storage[cache_key] = ::LruRedux::TTL::ThreadSafeCache.new(@lru_cache_size, @ttl)
    end
    @@cache_storage[cache_key]
  end

  def register
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

    @redis_builder ||= method(:internal_redis_builder)

    @identity = "#{@redis_url} :#{@prefix}"
    @logger.info("Registering Redis", :identity => @identity)
  end

  def filter(event)
    source = event.get(@source)
    cache = lookupCacheStorage(source)
    if !cache
      begin
        result = lookup(source)
        @@cache_storage[source] = result
        event.set(@target, result)
      rescue StandardError => e
        @logger.error("Uknown error while parsing referer data", :exception => e, :field => @source, :event => event)
        return
      end
    else
      result = cache
    end

    return unless result

  end

  def lookup(source)
    return unless source

    redis_key = "#{@prefix}_#{@source}"
    results = @redis.get(redis_key)

    results = @codec.decode(results)
    results
  end

  # private methods -----------------------------
  private

  # private
  def redis_params
    {
        :host => @host,
        :port => @port,
        :timeout => @timeout,
        :db => @db,
        :password => @password.nil? ? nil : @password.value
    }
  end

  # private
  def internal_redis_builder
    ::Redis.new(redis_params)
  end

  # private
  def connect
    redis = new_redis_instance
    redis
  end

  # def connect

  # private
  def stop
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end

  # private
  def redis_runner
    begin
      @redis ||= connect
      yield
    rescue ::Redis::BaseError => e
      @logger.warn("Redis connection problem", :exception => e)
      # Reset the redis variable to trigger reconnect
      @redis = nil
      Stud.stoppable_sleep(1) {stop?}
      retry if !stop?
    end
  end

# end

end # Tag Filters  LogStash
