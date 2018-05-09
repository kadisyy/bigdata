import org.apache.spark.SparkConf  
import org.apache.spark.SparkContext  
import org.apache.spark.sql.hive.HiveContext  
/** 
 * ͨ��spark sql����hive����Դ 
 */  
object SparkSQL2Hive {  
  
  def main(args: Array[String]): Unit = {  
    val conf = new SparkConf();  
    conf.setAppName("SparkSQL2Hive for scala")  
    conf.setMaster("spark://master1:7077")  
  
    val sc = new SparkContext(conf)  
    val hiveContext = new HiveContext(sc)  
    //�û�����  
    hiveContext.sql("use testdb")  
    hiveContext.sql("DROP TABLE IF EXISTS people")  
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")  
    //�ѱ������ݼ��ص�hive�У�ʵ���Ϸ��������ݿ�������Ҳ����ֱ��ʹ��HDFS�е�����  
    hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/local/sparkApps/SparkSQL2Hive/resources/people.txt' INTO TABLE people")  
    //�û�����  
    hiveContext.sql("use testdb")  
    hiveContext.sql("DROP TABLE IF EXISTS peopleScores")  
    hiveContext.sql("CREATE TABLE IF NOT EXISTS peopleScores(name STRING, score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")  
    hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/local/sparkApps/SparkSQL2Hive/resources/peopleScore.txt' INTO TABLE peopleScores")  
  
    /** 
     * ͨ��HiveContextʹ��joinֱ�ӻ���hive�е����ֱ���в��� 
     */  
   val resultDF = hiveContext.sql("select pi.name,pi.age,ps.score "  
                      +" from people pi join peopleScores ps on pi.name=ps.name"  
                      +" where ps.score>90");  
    /** 
     * ͨ��saveAsTable����һ��hive managed table�����ݵ�Ԫ���ݺ����ݼ����ŵľ���λ�ö����� 
     * hive���ݲֿ���й���ģ���ɾ���ñ��ʱ������Ҳ��һ��ɾ�������̵����ݲ��ٴ��ڣ� 
     */  
    hiveContext.sql("drop table if exists peopleResult")  
    resultDF.registerTempTable("peopleResult")  
  
    /** 
     * ʹ��HiveContext��table��������ֱ�Ӷ�ȡhive���ݲֿ��Table������DataFrame, 
     * ����������ѧϰ��ͼ���㡢���ָ��ӵ�ETL�Ȳ��� 
     */  
    val dataframeHive = hiveContext.table("peopleResult")  
    dataframeHive.show()  
  
  
  }  
}  