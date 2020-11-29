package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-23 9:39
  **/
object Scala03_Oper2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext("local", "Scala03_Oper2", new SparkConf());

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10);

    //mapPartitions
    //mapPartitions可以对一个map中所有的分区进行遍历
    //mapPartitions效率优于map算子，减少了发送到executor的次数
    //可能会出现内存溢出
    //返回Iterator；map的返回也是Iterator
    val partitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(_ * 2)
    })

    partitionsRDD.foreach(printf("%d \t", _))
  }
}
