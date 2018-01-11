# 几个例子(根据数据源分类)
说明：  
当前ELK5.0版本
# file   
input {
  file {
    path => "F:\opt\http.log"
	start_position => beginning
	
  }
}
filter {
  grok {
    match => { "message" => "%{IP:clientIP} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}" }
  }
}
output {
      elasticsearch { 
                        hosts => "localhost:9200"
						index => "test123" #索引名称
                    } 
        }
curl POST http://localhost:9200/test123 


# sql 
input {
    stdin {
    }
    jdbc {
      jdbc_connection_string => "jdbc:mysql://localhost:3306/mdas"
      jdbc_user => "root"
      jdbc_password => "root"
      jdbc_driver_library => "F:\software\bigdata_compents\elk_5.0\logstash-5.2.0\mysql-test\mysql-connector-java-5.1.42.jar"
      jdbc_driver_class => "com.mysql.jdbc.Driver"
      jdbc_paging_enabled => "true"
      jdbc_page_size => "100000"
      statement => "select * from es_log_kibana"
      schedule => "* * * * *"
      type => "jdbc"
    }
}
output {
    stdout {
        codec => json_lines
		}
	
	elasticsearch {
        hosts  => "localhost:9200"
        index => "test-1-11" #索引名称
        document_type => "form" #type名称
        document_id => "%{id}" #id必须是待查询的数据表的序列字段
		}
}
