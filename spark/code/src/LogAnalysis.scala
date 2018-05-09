import java.text.SimpleDateFormat  
  
import org.apache.spark.rdd.RDD  
import org.apache.spark.{SparkConf, SparkContext}  
  
import scala.util.matching.Regex  
  
/** 
  * 用户在线时长和登录次数统计 
  */  
object UserOnlineAnalysis {  
  def main(args: Array[String]) {  
 
  
    val conf = new SparkConf().setAppName("UserOnlineAnalysis").setMaster("yarn-client")  
    val sc = new SparkContext(conf)  
    //args(0)输入文件路径  
    val data = sc.textFile("hdfs://master:9000/spark/log/log")  
    //剔除type等于3的数据 imei为Unknown 为"" 为"000000000000000"的数据  
    val notContainsType3 = data.filter(!_.contains("\\\"type\\\":\\\"3\\\"")).filter(!_.contains("\\\"imei\\\":\\\"\\\"")).filter(!_.contains("000000000000000")).filter(!_.contains("Unknown"))  
    //过滤logid或imei不存在的数据 \"imei\":\"\"  
    val cleanData = notContainsType3.filter(_.contains("logid")).filter(_.contains("imei"))  
  
    val cleanMap = cleanData.map {  
      line =>  
          val data = formatLine(line).split(",")  
        (data(0), data(1))  
    }  
    //RDD的数据安装IMEI号分组并且按照imei号排序,输出时每行分组的第二个元素列表按照时间排序sortByKey().  
    val rdd = cleanMap.groupByKey().map(x => (x._1, x._2.toList.sorted))  
  
    rdd.cache()  
  
    //导出明细  
    exportDetailData(rdd,  "hdfs://master:9000/spark/detail")  
  
    //导出统计  
    exportSumData(rdd, "hdfs://master:9000/spark/sum")  
  
  
    rdd.unpersist()  
  
    sc.stop()  
  
  }  
  
  /** 
    * 导出用户在线时长和登录次数统计结果 
    * 存储结构:(IMEI,登录次数,在线时长(秒)) 
    * 
    **/  
  def exportSumData(map: RDD[(String, List[String])], output: String): Unit = {  
    val result = map.map {  
      x =>  
        //登录次数,默认登录1次  
        var logNum: Int = 1  
        //在线时长(秒)  
        var totalTime: Long = 0  
  
        val len = x._2.length  
  
        for (i <- 0 until len) {  
          if (i + 1 < len) {  
            val nowTime = getTimeByString(x._2(i))  
            val nextTime = getTimeByString(x._2(i + 1))  
            val intervalTime = nextTime - nowTime  
            if (intervalTime < 60 * 10) {  
              totalTime += intervalTime  
            } else {  
              logNum += 1  
            }  
          }  
  
        }  
        //输出ime,登录次数,总时长(秒)  
        (x._1, logNum, totalTime)  
    }  
  
    result.saveAsTextFile(output)  
  }  
  
  /** 
    * 导出用户在线时长和首次登录时间 
    * 存储结构:(IMEI,首次登录时间,在线时长(秒)) 
    * 
    **/  
  def exportDetailData(map: RDD[(String, List[String])], output: String): Unit = {  
    val result = map.flatMap {  
      x =>  
        val len = x._2.length  
        val array = new Array[(String, String, Long)](len)  
        for (i <- 0 until len) {  
          if (i + 1 < len) {  
            val nowTime = getTimeByString(x._2(i))  
            val nextTime = getTimeByString(x._2(i + 1))  
            val intervalTime = nextTime - nowTime  
            if (intervalTime < 60 * 10) {  
              array(i) = (x._1, x._2(i), intervalTime)  
            } else {  
              array(i) = (x._1, x._2(i), 0)  
            }  
          } else {  
            array(i) = (x._1, x._2(i), 0)  
          }  
  
        }  
        array  
    }  
    result.saveAsTextFile(output)  
  }  
  
  /** 
    * 从每行日志解析出imei和logid 
    * 
    **/  
  def formatLine(line: String): String = {  
      val logIdRegex = """"logid":"([0-9]+)",""".r  
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r  
    val logId = getDataByPattern(logIdRegex, line)  
    val imei = getDataByPattern(imeiRegex, line)  
  
    //时间取到秒  
    imei + "," + logId.substring(0, 14)  
  }  
  /** 
    * 根据正则表达式,查找相应值 
    * 
    **/  
  def getDataByPattern(p: Regex, line: String): String = {  
    val result = (p.findFirstMatchIn(line)).map(item => {  
      val s = item group 1 //返回匹配上正则的第一个字符串。  
      s  
    })  
    result.getOrElse("NULL")  
  }  
  /** 
    * 根据时间字符串获取时间秒数,单位(秒) 时间戳是指格林威治时间1970年01月01日00时00分00秒(北京时间1970年01月01日08时00分00秒)起至现在的总毫秒数 
    * 所以返回时间戳/1000 
    **/  
  def getTimeByString(timeString: String): Long = {  
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  
    sf.parse(timeString).getTime / 1000  
  }  
}  