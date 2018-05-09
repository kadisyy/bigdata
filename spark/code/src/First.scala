
import org.apache.spark.{SparkConf, SparkContext} 
object First {
  def main(args: Array[String]): Unit = {  
    val conf = new SparkConf().setMaster("local").setAppName("testpartitions") 
    val sc = new SparkContext(conf)
    val file=sc.textFile("E:\\BigData\\test\\input\\wordcount.txt")
    println("����1��"+file.getNumPartitions);
    val count=file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    println("����2��"+count.getNumPartitions);

    count.saveAsTextFile("E:\\BigData\\test\\output")
   
  }  
}