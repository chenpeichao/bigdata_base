package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author ceek
  * @create 2020-11-26 11:11
  **/
object Scala14_Oper13 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("scala14_oper13");

    val sc = new SparkContext(sparkConf);

    val listRDD1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    val listRDD2: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 1), ("d", 2)))


    val partitionByRDD: RDD[(String, Int)] = listRDD2.partitionBy(new MyPartition(5))

    partitionByRDD.glom().collect().foreach(x => {
      x.foreach(println); println("-------")
    })
  }
}

//声明分区器
//继承Partitioner方法
class MyPartition(key: Int = 3) extends Partitioner {
  //返回创建出来的分区数
  def numPartitions: Int = {
    println("调用获取分区数方法！")
    key
  }

  //返回给定键的分区编号，可动态将数据指定到具体分区
  def getPartition(key: Any): Int = {
    if (key.isInstanceOf[String]) {
      if (key.asInstanceOf[String].equals("a")) {
        1
      } else {
        2
      }
    } else {
      3
    }
  }
}
