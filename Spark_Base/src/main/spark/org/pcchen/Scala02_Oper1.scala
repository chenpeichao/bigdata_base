package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-23 9:27
  **/
object Scala02_Oper1 {
  def main(args: Array[String]): Unit = {
    //构造sparkconf参数
    var sparkConf = new SparkConf();
    //    sparkConf.setMaster("local");
    //    sparkConf.setAppName("Scala02_Oper1")

    //初始化spark上下文对象，并指定RDD的运行模式
    var sc = new SparkContext("local", "Scala02_Oper1", sparkConf);

    val datasRDD: RDD[Int] = sc.makeRDD(1 to 5);

    //map算子
    val resRDD: RDD[Int] = datasRDD.map(_ * 2);

    resRDD.collect().foreach(printf("%d\t", _));
  }
}
