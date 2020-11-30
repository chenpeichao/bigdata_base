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

    //创建自定义累加器
    val myAccumulator = new MyAccumulator();
    //注册累加器
    sc.register(myAccumulator)

    listRDD.foreach(x => {
      //执行累加器的累加功能
      myAccumulator.add(x)
    })

    //获得累加器的值
    println(myAccumulator.value);
  }
}

//声明累加器
//1、继承AccumulatorV2
//2、实现抽象方法
//3、创建累加器
class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  var list = new util.ArrayList[String]();

  // 当前累加器是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制累加器状态
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    /*var myAccumulator = new MyAccumulator();
    myAccumulator.list = this.list
    myAccumulator*/
    new MyAccumulator();
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累计其中增加数据
  override def add(v: String): Unit = {
    if (v.startsWith("f")) {
      list.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器中数据
  override def value: util.ArrayList[String] = list
}