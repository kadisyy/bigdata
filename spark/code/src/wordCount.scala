import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://master:9000/data")
    val count=file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    
    count.collect()
    
  }
}