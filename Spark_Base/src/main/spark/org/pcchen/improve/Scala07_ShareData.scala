package org.pcchen.improve

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享变量：使用系统自带累加器
  *
  * @author ceek
  * @create 2020-11-30 15:50
  **/
object Scala07_ShareData {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Scala07_ShareData");

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[Int] = sc.makeRDD(1 to 5, 2);

    val accumulator: LongAccumulator = sc.longAccumulator;

    listRDD.foreach(x => {
      accumulator.add(x);
    });

    println(accumulator.value);
    sc.stop()
  }
}
