# logstash-filter-redis
logstash redis filter 从redis取出对应的数据添加到event中(比如可实现对ip添加业务tag的功能)

## 例子
```ruby
filter {
     redis {
       host => "127.0.0.1"
       port => 6379
       db => 0
       # password => ""
       action => "GET"
       key => "%{host}"
       field => "%{host}"
       name => "ext"
       default => ""
     }
}
```

