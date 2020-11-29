package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-27 16:11
  **/
object Scala21_Oper20 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();

    sparkConf.setMaster("local")
    sparkConf.setAppName("Scala21_Oper20")

    val sc = new SparkContext(sparkConf);
    //初始化数据

    val listRDD = sc.parallelize(List((1, "a"), (1, "d"), (2, "b"), (3, "c")), 2);
    listRDD.glom().collect().foreach(x => {
      x.foreach(println);
      println("-------");
    })

    val mapValuesRDD: RDD[(Int, String)] = listRDD.mapValues(x => x + "|||")

    mapValuesRDD.collect().foreach(println)
  }
}
