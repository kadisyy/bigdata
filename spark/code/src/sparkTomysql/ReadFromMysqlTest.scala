package sparkTomysql

import java.util.Properties  
  
import org.apache.spark.sql.{DataFrame, SQLContext}  
import org.apache.spark.{SparkConf, SparkContext}  
  
/**  
  * �������������ύ����  
  * spark-submit --class sql.SparkSqlMysqlDatasource --master yarn-cluster --executor-memory 2G --num-executors 2 --driver-memory 1g --executor-cores 1  /data1/e_heyutao/sparktest/sparkEnn.jar  
  *  
  */  
object ReadFromMysqlTest {  
  //���ݿ�����  
  lazy val url = "jdbc:mysql://localhost:3306/bigdata"  
  lazy val username = "root"  
  lazy val password = "root"  
  
  def main(args: Array[String]) {  
//    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[2]").set("spark.app.id", "sql")  
    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local").set("spark.app.id", "sqlTest")  
    //���л�  
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
    sparkConf.set("spark.kryoserializer.buffer", "256m")  
    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")  
    sparkConf.set("spark.akka.frameSize", "500")  
    sparkConf.set("spark.rpc.askTimeout", "30")  
    //��ȡcontext  
    val sc = new SparkContext(sparkConf)  
    //��ȡsqlContext  
    val sqlContext = new SQLContext(sc)  
  
    //������ʽת��������ʹ��spark sql���ú���  
    import sqlContext.implicits._  
      
    //����jdbc������Ϣ  
    val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"  
    val prop = new Properties()  
    //ע�⣺��Ⱥ������ʱ��һ��Ҫ�����仰������ᱨ�Ҳ���mysql�����Ĵ���  
    prop.put("driver", "com.mysql.jdbc.Driver")  
    //����mysql���ݱ�  
    val df_test2: DataFrame = sqlContext.read.jdbc(uri, "operationsql", prop)  
  
    //��dataframe�л�ȡ�����ֶ�  
    df_test2.select("name", "count").collect()  
      .foreach(row => {
        println("name  " + row(0) + ", age  " + row(1))  
      })  
  
    /**  
      * ע�⣺�鿴Դ�����֪����ϸ��˼  
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