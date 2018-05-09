import java.text.SimpleDateFormat  
  
import org.apache.spark.rdd.RDD  
import org.apache.spark.{SparkConf, SparkContext}  
  
import scala.util.matching.Regex  
  
/** 
  * �û�����ʱ���͵�¼����ͳ�� 
  */  
object UserOnlineAnalysis {  
  def main(args: Array[String]) {  
 
  
    val conf = new SparkConf().setAppName("UserOnlineAnalysis").setMaster("yarn-client")  
    val sc = new SparkContext(conf)  
    //args(0)�����ļ�·��  
    val data = sc.textFile("hdfs://master:9000/spark/log/log")  
    //�޳�type����3������ imeiΪUnknown Ϊ"" Ϊ"000000000000000"������  
    val notContainsType3 = data.filter(!_.contains("\\\"type\\\":\\\"3\\\"")).filter(!_.contains("\\\"imei\\\":\\\"\\\"")).filter(!_.contains("000000000000000")).filter(!_.contains("Unknown"))  
    //����logid��imei�����ڵ����� \"imei\":\"\"  
    val cleanData = notContainsType3.filter(_.contains("logid")).filter(_.contains("imei"))  
  
    val cleanMap = cleanData.map {  
      line =>  
          val data = formatLine(line).split(",")  
        (data(0), data(1))  
    }  
    //RDD�����ݰ�װIMEI�ŷ��鲢�Ұ���imei������,���ʱÿ�з���ĵڶ���Ԫ���б���ʱ������sortByKey().  
    val rdd = cleanMap.groupByKey().map(x => (x._1, x._2.toList.sorted))  
  
    rdd.cache()  
  
    //������ϸ  
    exportDetailData(rdd,  "hdfs://master:9000/spark/detail")  
  
    //����ͳ��  
    exportSumData(rdd, "hdfs://master:9000/spark/sum")  
  
  
    rdd.unpersist()  
  
    sc.stop()  
  
  }  
  
  /** 
    * �����û�����ʱ���͵�¼����ͳ�ƽ�� 
    * �洢�ṹ:(IMEI,��¼����,����ʱ��(��)) 
    * 
    **/  
  def exportSumData(map: RDD[(String, List[String])], output: String): Unit = {  
    val result = map.map {  
      x =>  
        //��¼����,Ĭ�ϵ�¼1��  
        var logNum: Int = 1  
        //����ʱ��(��)  
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
        //���ime,��¼����,��ʱ��(��)  
        (x._1, logNum, totalTime)  
    }  
  
    result.saveAsTextFile(output)  
  }  
  
  /** 
    * �����û�����ʱ�����״ε�¼ʱ�� 
    * �洢�ṹ:(IMEI,�״ε�¼ʱ��,����ʱ��(��)) 
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
    * ��ÿ����־������imei��logid 
    * 
    **/  
  def formatLine(line: String): String = {  
      val logIdRegex = """"logid":"([0-9]+)",""".r  
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r  
    val logId = getDataByPattern(logIdRegex, line)  
    val imei = getDataByPattern(imeiRegex, line)  
  
    //ʱ��ȡ����  
    imei + "," + logId.substring(0, 14)  
  }  
  /** 
    * ����������ʽ,������Ӧֵ 
    * 
    **/  
  def getDataByPattern(p: Regex, line: String): String = {  
    val result = (p.findFirstMatchIn(line)).map(item => {  
      val s = item group 1 //����ƥ��������ĵ�һ���ַ�����  
      s  
    })  
    result.getOrElse("NULL")  
  }  
  /** 
    * ����ʱ���ַ�����ȡʱ������,��λ(��) ʱ�����ָ��������ʱ��1970��01��01��00ʱ00��00��(����ʱ��1970��01��01��08ʱ00��00��)�������ڵ��ܺ����� 
    * ���Է���ʱ���/1000 
    **/  
  def getTimeByString(timeString: String): Long = {  
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  
    sf.parse(timeString).getTime / 1000  
  }  
}  