package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-23 10:13
  **/
object Scala04_Oper3 {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local[4]", "Scala04_Oper3", new SparkConf());

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13));

    //mapPartitionsWithIndex可以获取到数据所在分区的索引
    val partitionIndexRDD: RDD[(Int, Int)] = listRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map((_, index))
    })

    partitionIndexRDD.collect().foreach(println)
  }
}
