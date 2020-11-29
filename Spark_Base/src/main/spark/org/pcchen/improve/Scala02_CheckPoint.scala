package main.spark.org.pcchen.improve

import org.apache.spark.{SparkConf, SparkContext}

object Scala02_CheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[2]");
    sparkConf.setAppName("Scala02_CheckPoint");

    val sc = new SparkContext(sparkConf);

    sc.setCheckpointDir("Spark_Base/checkpoint")
    val listRDD = sc.parallelize(List("hadoop", "scala", "hadoop", "hive"));

    val wordCountRDD = listRDD.map((_, 1));
    wordCountRDD.checkpoint();

    println(wordCountRDD.toDebugString)

    val wordCountRDD1 = wordCountRDD.reduceByKey(_ + _);
    println(wordCountRDD1.toDebugString)

    wordCountRDD.collect()
    //    wordCountRDD.collect().foreach(println)
    //    wordCountRDD1.collect().foreach(println)
    println(wordCountRDD1.toDebugString)
  }
}
