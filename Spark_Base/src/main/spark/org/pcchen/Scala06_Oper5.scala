package main.spark.org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala06_Oper5 {
  def main(args: Array[String]): Unit = {
    //创建spark上下文对象
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala06_Oper5");

    val sc = new SparkContext(sparkConf)
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))

    //将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(x => println(x.mkString(" ")))
  }
}
