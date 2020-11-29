package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * foldByKey：是分区内和分区间运算相同的aggregateByKey；即简化版
  */
object Scala18_Oper17 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala16_Oper15");

    val sc = new SparkContext(sparkConf);

    val strRDD = sc.parallelize(List(("a", 2), ("d", 4), ("b", 3), ("a", 2)), 2);

    //简化版的aggregateByKey运算：即分区内运算和分区间运算相同
    val foldByKeyRDD: RDD[(String, Int)] = strRDD.foldByKey(2)(_ + _);

    foldByKeyRDD.collect().foreach(println);
  }
}
