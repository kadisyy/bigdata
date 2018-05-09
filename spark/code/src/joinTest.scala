import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

object joinTest extends App{

  val sparkConf = new SparkConf().setAppName("joinTest")
  
   sparkConf.setMaster("spark://master:7077");

  val sc = new SparkContext(sparkConf)

  /**
   * map-side-join
   * ȡ��С���г��ֵ��û����������ȡ������Ҫ����Ϣ
   * */
  //��������Ϣ(���֤,����)
  val people_info = sc.parallelize(Array(("110","lsw"),("112","yyy"))).collectAsMap()
  //ȫ����ѧ����ϸ��Ϣ(���֤,ѧУ����,ѧ��...)
  val student_all = sc.parallelize(Array(("110","s1","211"),
                                              ("111","s2","222"),
                                              ("112","s3","233"),
                                              ("113","s2","244")))

  //����Ҫ������С����й���
  val people_bc = sc.broadcast(people_info)

  /**
   * ʹ��mapPartition��������map�����ٴ���broadCastMap.value�Ŀռ�����
   * ͬʱƥ�䲻��������Ҳ����Ҫ���أ���
   * */
  val res = student_all.mapPartitions(iter =>{
    val stuMap = people_bc.value
    val arrayBuffer = ArrayBuffer[(String,String,String)]()
    iter.foreach{case (idCard,school,sno) =>{
      if(stuMap.contains(idCard)){
        arrayBuffer.+= ((idCard, stuMap.getOrElse(idCard,""),school))
      }
    }}
    arrayBuffer.iterator
  })

  /**
   * ʹ����һ�ַ�ʽʵ��
   * ʹ��for������
   * */
  val res1 = student_all.mapPartitions(iter => {
    val stuMap = people_bc.value
    for{
      (idCard, school, sno) <- iter
      if(stuMap.contains(idCard))
    } yield (idCard, stuMap.getOrElse(idCard,""),school)
  })

  res.foreach(println)
  
}

