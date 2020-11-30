package org.pcchen.improve

import java.{util}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

/**
  * @author ceek
  * @create 2020-11-30 16:09
  **/
object Scala08_Accumulator {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Scala08_Accumulator");

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[String] = sc.makeRDD(List("first", "second", "third"), 2)

    val myAccumulator = new MyAccumulator();
    sc.register(myAccumulator)

    listRDD.foreach(x => {
      myAccumulator.add(x)
    })

    println(myAccumulator.value);
  }
}

class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]();

  override def isZero: Boolean = {
    list.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new MyAccumulator();
  }

  override def reset(): Unit = {
    list.clear()
  }

  override def add(v: String): Unit = {
    if (v.startsWith("f")) {
      list.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = list
}