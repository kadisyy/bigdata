package sparkStatistics
import org.apache.spark.{SparkConf, SparkContext} 

object statistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("statistics").setMaster("local")
    val sc = new SparkContext(conf)
    //加载数据集
val rdd1 = sc.textFile("/home/shiyanlou/data.txt")

//过滤
val rdd2 = rdd1.filter (log => log.split(",").length == 3)

//切分字段
//导入 Row
import org.apache.spark.sql.Row
//取前两个字段
val rowRdd3 = rdd2.map { log => Row(log.split(",")(0), log.split(",")(1).toDouble) }

//导入
import org.apache.spark.sql.types._

//创建映射对应关系
val schema = StructType(Array(
StructField("date", StringType, true),
StructField("sale_amount", DoubleType, true))
)

// 创建df
val saleDF = spark.createDataFrame(rowRdd3, schema)  
import spark.implicits._

saleDF.groupBy("date").agg('date, sum('sale_amount)).rdd.map { row => Row(row(1), row(2)) }.coalesce(1,true).saveAsTextFile("/home/shiyanlou/res")

  }
}