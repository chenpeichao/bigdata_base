package main.spark.org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala01_RDD {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("scala01_rdd")

    val sc: SparkContext = new SparkContext(sparkConf)

    //创建RDD
    // 1) 从内存中创建 makeRDD，底层实现就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4));
    sc.makeRDD(Array(1), 2)
    val arrRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5));

    //2) 从内存中创建parallelize
    val paraListRDD: RDD[Int] = sc.parallelize(List(5, 6, 7, 8))

    //3) 从外部存储中创建
    //默认情况下，可以读取项目路径，也可以读取其它路径：HDFS
    //默认从文件中读取的数据都是字符串类型
    //读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时分片规则
    val fileRDD: RDD[String] = sc.textFile("Spark_Base/in", 8)

    fileRDD.saveAsTextFile("Spark_Base/output");
  }
}