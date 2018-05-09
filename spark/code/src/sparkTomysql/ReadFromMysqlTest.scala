package sparkTomysql

import java.util.Properties  
  
import org.apache.spark.sql.{DataFrame, SQLContext}  
import org.apache.spark.{SparkConf, SparkContext}  
  
/**  
  * 生产环境：下提交任务  
  * spark-submit --class sql.SparkSqlMysqlDatasource --master yarn-cluster --executor-memory 2G --num-executors 2 --driver-memory 1g --executor-cores 1  /data1/e_heyutao/sparktest/sparkEnn.jar  
  *  
  */  
object ReadFromMysqlTest {  
  //数据库配置  
  lazy val url = "jdbc:mysql://localhost:3306/bigdata"  
  lazy val username = "root"  
  lazy val password = "root"  
  
  def main(args: Array[String]) {  
//    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[2]").set("spark.app.id", "sql")  
    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local").set("spark.app.id", "sqlTest")  
    //序列化  
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
    sparkConf.set("spark.kryoserializer.buffer", "256m")  
    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")  
    sparkConf.set("spark.akka.frameSize", "500")  
    sparkConf.set("spark.rpc.askTimeout", "30")  
    //获取context  
    val sc = new SparkContext(sparkConf)  
    //获取sqlContext  
    val sqlContext = new SQLContext(sc)  
  
    //引入隐式转换，可以使用spark sql内置函数  
    import sqlContext.implicits._  
      
    //创建jdbc连接信息  
    val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"  
    val prop = new Properties()  
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误  
    prop.put("driver", "com.mysql.jdbc.Driver")  
    //加载mysql数据表  
    val df_test2: DataFrame = sqlContext.read.jdbc(uri, "operationsql", prop)  
  
    //从dataframe中获取所需字段  
    df_test2.select("name", "count").collect()  
      .foreach(row => {
        println("name  " + row(0) + ", age  " + row(1))  
      })  
  
    /**  
      * 注意：查看源码可以知道详细意思  
    def mode(saveMode: String): DataFrameWriter = {  
          this.mode = saveMode.toLowerCase match {  
          case "overwrite" => SaveMode.Overwrite  
          case "append" => SaveMode.Append  
          case "ignore" => SaveMode.Ignore  
          case "error" | "default" => SaveMode.ErrorIfExists  
          case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +  
            "Accepted modes are 'overwrite', 'append', 'ignore', 'error'.")  
    }  
      */ 
  
  }  
}