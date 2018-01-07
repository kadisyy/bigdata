# 0 准备配置文件  
file_es.conf  
input {  
        file {  
          type => "messagelog"  
          path => "/var/log/messages"  
          start_position => "beginning"  
        }  
}  
output {  
        file {  
          path => "/tmp/123.txt"  
        }  
        elasticsearch {  
                hosts => ["192.168.86.133:9200"]  
                index => "system-messages-%{+yyyy.MM.dd}"  
        }  
}  


# 1 启动logstash命令  

./bin/logstash -e config/file_es.conf  


简单说明:  
从var/log/message获取日志  
日志存储在/tmp/123.txt 和 ES索引2个位置  data目录  
ES 索引名称:system-messages-2018.01.07  
curl POST http://localhost:9200/system-messages-2018.01.07  
