
package sparkTomysql

import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object RDDtoMysql {
    case class Blog(name: String, count: Int)
 
  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var ps:PreparedStatement = null
    var conn: Connection = null
    val sql = "insert into operationsql(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root", "root")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
      }
    catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
 
  def main(args: Array[String]) {
    //设置运行模式和应该名称
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val file=sc.textFile("hdfs://master:9000/spark/wordcount.txt") 
    val count=file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_).sortBy(_._2, false)
   
    val data = sc.parallelize(count.collect())
    data.foreachPartition(myFun)
  }
}