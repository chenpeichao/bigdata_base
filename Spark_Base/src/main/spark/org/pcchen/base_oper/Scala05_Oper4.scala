package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-23 10:20
  **/
object Scala05_Oper4 {
  def main(args: Array[String]): Unit = {
    //构造sparkconf参数
    var sparkConf = new SparkConf();
    //    sparkConf.setMaster("local");
    //    sparkConf.setAppName("Scala02_Oper1")

    //初始化spark上下文对象，并指定RDD的运行模式
    var sc = new SparkContext("local", "Scala05_Oper4", sparkConf);

    val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(2, 3, 4), List(11, 12)));

    val res: RDD[Int] = listRDD.flatMap(datas => datas)

    println(res.collect().foreach(printf("%d\t", _)))
  }
}
