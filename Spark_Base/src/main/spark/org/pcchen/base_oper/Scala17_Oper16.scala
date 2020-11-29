package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregateByKey案例
  */
object Scala17_Oper16 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala16_Oper15");

    val sc = new SparkContext(sparkConf);

    val strRDD = sc.parallelize(List(("a", 2), ("d", 4), ("b", 3), ("a", 2)), 2);

    strRDD.glom().collect().foreach(x => {
      x.foreach(println);
      println("----");
    })
    val aggRDD: RDD[(String, Int)] = strRDD.aggregateByKey(1)(_ + _, _ + _)
    //
    val aggRDDValue = strRDD.aggregate(2)((x, y) => x + y._2, _ + _)

    aggRDD.collect().foreach(println)
    println(aggRDDValue)
  }
}
