package main.spark.org.pcchen.improve

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 序列化练习
  */
object Scala01_SeriTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[2]");
    sparkConf.setAppName("Scala01_SeriTest");

    val sc = new SparkContext(sparkConf);

    val listRDD = sc.parallelize(List("hadoop", "scala", "hive"));

    var search = new Search("sc");
    val searchRDD = search.getMatch2(listRDD);

    println(listRDD.toDebugString);
    println(searchRDD.toDebugString);
    println(searchRDD.dependencies);

    searchRDD.collect().foreach(println);
  }
}

//class Search(query:String) extends Serializable {
class Search(query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]): RDD[String] = {
    //此处调用的isMatch方法是通过this.isMatch即是对象的，程序运行需要将Search对象传递到Executor上，故Search需要实现序列化来保证对象网络传输
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //由于query变量是对象持有的，contains算子是需要到Executor上执行，所以需要对对象实现序列化才能实现网络间的传递；
    //或将变量定义成另一变量，进行网络间的传递
    val queryTmp = query;
    rdd.filter((param: String) => param.contains(queryTmp))
  }
}