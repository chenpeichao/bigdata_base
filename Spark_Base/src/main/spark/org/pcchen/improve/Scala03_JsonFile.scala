package org.pcchen.improve

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-30 10:14
  **/
object Scala03_JsonFile {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Scala03_JsonFile", new SparkConf())

    val jsonFileRDD: RDD[String] = sc.textFile("Spark_Base/in/jsonfile")

    jsonFileRDD.collect().foreach(println)
  }
}
