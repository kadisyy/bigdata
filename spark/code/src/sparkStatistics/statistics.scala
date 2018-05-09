package sparkStatistics
import org.apache.spark.{SparkConf, SparkContext} 

object statistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("statistics").setMaster("local")
    val sc = new SparkContext(conf)
    //�������ݼ�
val rdd1 = sc.textFile("/home/shiyanlou/data.txt")

//����
val rdd2 = rdd1.filter (log => log.split(",").length == 3)

//�з��ֶ�
//���� Row
import org.apache.spark.sql.Row
//ȡǰ�����ֶ�
val rowRdd3 = rdd2.map { log => Row(log.split(",")(0), log.split(",")(1).toDouble) }

//����
import org.apache.spark.sql.types._

//����ӳ���Ӧ��ϵ
val schema = StructType(Array(
StructField("date", StringType, true),
StructField("sale_amount", DoubleType, true))
)

// ����df
val saleDF = spark.createDataFrame(rowRdd3, schema)  
import spark.implicits._

saleDF.groupBy("date").agg('date, sum('sale_amount)).rdd.map { row => Row(row(1), row(2)) }.coalesce(1,true).saveAsTextFile("/home/shiyanlou/res")

  }
}