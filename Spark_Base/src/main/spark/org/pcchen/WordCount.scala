package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-19 10:16
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("ceek_wordCount")
    sparkConf.setMaster("local")
//    sparkConf.set("spark.master", "local");

    val sc: SparkContext = new SparkContext(sparkConf)

    val file: RDD[String] = sc.textFile("Spark_Base/in")

    val file1 = file.map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).collect()
    file1.foreach(println(_))
  }
}
