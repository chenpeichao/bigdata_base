package org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrame的手动创建方式
  *
  * @author ceek
  * @create 2020-12-02 11:27
  **/
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL01_Demo");

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    val value: RDD[String] = spark.sparkContext.textFile("")

    val df: DataFrame = spark.read.json("Spark_Base/in/jsonFile")

    df.show();

    spark.stop();
  }
}
