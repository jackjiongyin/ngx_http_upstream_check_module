# ngx\_http\_upstream\_check\_module

## 简介

nginx主动探活模块， 使用原生`Nginx API`实现。相对于`Tengine`, 对`Nginx`源码改动比较小，方便后续软件的升级维护

## 使用示例

    upstream test {
        server 127.0.0.1:8888;      
        health_check type=http interval=10000s fail=5 rise=1;
        http_send method=head uri=/ protocol=1.1;
        check_http_expect_alive 2xx 3xx;
    }
    
    server {
        location / {
            proxy_pass http://test;
        }
    }
    
## 配置指令

###

    Syntax:  health_check [type=check_type] [interval=milliseconds] [timeout=milliseconds] [fail=count] [rise=count];
    Default: type=http interval=10000s timeout=1000s fail=5 rise=1 slow_start=30s
    Context: upstream
    
`health_check`指令开启健康检查模块(默认不开启), 其参数可不显示定义，没有明显定义时，将使用默认参数，以下是参数说明：

* `type`: 健康检查类型，目前支持`[tcp|http]`, 默认`tcp`
* `interval`: 主动检测时间间隔， 默认`5s(5000)`
* `timeout`: 请求超时时间，默认`1s(1000)`
* `fail`: 如果连续失败次数达到`fall`的次数，服务器就被认为是`down`，默认2次
* `rise`: 如果连续成功次数达到`rise`的次数，服务器就被认为是`up`， 默认5次

###
    
    Syntax:  http_send HTTP-Request;
    Default: http_send HEAD / HTTP/1.0\r\n\r\n
    Context: upstream
        
`http_send `只在`health_check`配置的类型为`http`时才生效

###
    
    Syntax:  alive_http_status [expect_service_alive_status];
    Default: 2xx|3xx|4xx|5xx
    Context: upstream
    
`alive_http_status `只在`health_check`配置的类型为`http`时才生效，该指令指定HTTP回复的成功状态，默认认为2XX和3XX的状态是健康的。

## LICENSE

`MIT License`
