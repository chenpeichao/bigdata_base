package org.pcchen

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-26 17:06
  **/
object Scala15_Oper14 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala15_Oper14");

    val sc = new SparkContext(sparkConf);

    val listRDD = sc.parallelize(List(("a", 1), ("b", 2), ("c", 1), ("d", 2)));

    listRDD.groupByKey
  }
}
