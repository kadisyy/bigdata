import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * �����������ڹ�����Ʒ�ϵͳ�У��������߹��˵��������ĵ����������������̵����棬
  * ֻ������Ч�Ĺ�����Ʒѡ������ڷ�ˢ���֣�����������ϵͳ,���˵���Ч��ͶƱ�������ֻ���������
  * ʵ�ּ�����ʹ��transform APIֱ�ӻ���RDD��̣�����join����
  *
  * Created by Administrator on 2016/4/30.
  */

object OnlineBlackListFilter {
  def main(args: Array[String]) {
    /**
      * ��һ��������Spark�����ö�������Spark���������ʱ��������Ϣ
      * ����˵ͨ��setMaster�����ó���Ҫ���ӵ�spark��Ⱥ��master��url���������Ϊ
      * local�� �����Spark�����ڱ������У��ر��ʺ��ڻ������������ǳ���
      * ������ֻ��1g���ڴ棩�ĳ�ѧ��
      */

    val conf = new SparkConf() //����SparkConf����
    conf.setAppName("OnlineBlackListFilter") //����SparkӦ�ó�������ƣ��ڳ������еļ�ؽ�����Կ�������
    //    conf.setMaster("local") //��ʱ�������ڱ������У�����Ҫ��װSpark��Ⱥ
    conf.setMaster("spark://master:7077") //��ʱ�������ڱ������У�����Ҫ��װSpark��Ⱥ

    val ssc = new StreamingContext(conf, Seconds(300))

    /**
      * ����������׼����ʵ���Ϻ�����һ�㶼�Ƕ�̬�ģ�������Redis�л������ݿ��У������������������и��ӵ�ҵ���߼���
      * ��������㷨��ͬ��������SparkStreaming���д����ʱ��ÿ�ζ��ܹ�������������Ϣ
      *
      */

    val blackList = Array(("hadoop", true), ("mahout", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)

    val adsClickStream = ssc.socketTextStream("master", 9999)

    /**
      * �˴�ģ��Ĺ������ÿ�����ݵĸ�ʽΪ��time��name
      * �˴�map�����Ľ����name, (time, name)�ĸ�ʽ
      */

    val adsClickStreamFormatted = adsClickStream.map(ads =>(ads.split(" ")(1), ads))
    adsClickStreamFormatted.transform(userClickRDD =>{
      //ͨ��leftOuterJoin�����ȱ���������û���������ݵ�RDD���������ݣ��ֻ������Ӧ��������Ƿ��ں�������
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)
      val validClicked = joinedBlackListRDD.filter(joinedItem => {
        /**
          *����filter���˵�ʱ��������Ԫ����һ��Tuple����name,((time, name), boolean)��
          * ���е�һ��Ԫ���Ǻ����������ƣ��ڶ���Ԫ�صĵڶ���Ԫ���ǽ���leftOuterJoin��ʱ���Ƿ���ڸ�ֵ
          * ������ڵĻ���������ǰ������Ǻ���������Ҫ���˵�������Ļ�������Ч������ݣ�
          */

        if(joinedItem._2._2.getOrElse(false)){
          false
        } else {
          true
        }
      })
      validClicked.map(validClicked =>{ validClicked._2._1 })
    }).print()
    /**
      * ��������Ч����һ�㶼��д��Kafka�У����εļƷ�ϵͳ���Kafka��pull����Ч���ݽ��мƷ�
      */

    ssc.start()
    ssc.awaitTermination()
  }
}