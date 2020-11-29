package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala16_Oper15 {
  //reduceByKey
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala16_Oper15");

    val sc = new SparkContext(sparkConf);

    val strRDD = sc.parallelize(List("a", "b", "c", "d", "a", "b", "c", "d"));
    val ListRDD = strRDD.map(x => (x, 1));

    val reduceByKeyRDD: RDD[(String, Int)] = ListRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)
  }
}
