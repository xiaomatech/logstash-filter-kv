require "logstash/devutils/rspec/spec_helper"
require "redis"
require "stud/try"
require 'logstash/filters/tag'
require 'securerandom'

def populate(key, event_count)
  require "logstash/event"
  redis = Redis.new(:host => "localhost")
  event_count.times do |value|
    event = LogStash::Event.new("sequence" => value)
    Stud.try(10.times) do
      redis.rpush(key, event.to_json)
    end
  end
end

def process(conf, event_count)
  events = input(conf) do |pipeline, queue|
    event_count.times.map {queue.pop}
  end

  expect(events.map {|evt| evt.get("sequence")}).to eq((0..event_count.pred).to_a)
end