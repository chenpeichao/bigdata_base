package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ceek
  * @create 2020-11-27 15:36
  **/
object Scala20_Oper19 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();

    sparkConf.setMaster("local")
    sparkConf.setAppName("Scala20_Oper19")

    val sc = new SparkContext(sparkConf);
    //初始化数据

    val listRDD = sc.parallelize(List(("a", 10), ("b", 10), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2);
    listRDD.glom().collect().foreach(x => {
      x.foreach(println); println("-------");
    })

    //    listRDD.sortByKey(false).collect().foreach(println)

    //    listRDD.sortBy(x=>x._1, false).collect().foreach(println)

    //二次排序延展可以多次排序
    val secondSortRDD: RDD[(SecondOrderKey, (String, Int))] = listRDD.map(x => (new SecondOrderKey(x._1, x._2), x))
    secondSortRDD.sortByKey(ascending = false).map(_._2).collect().foreach(println)
  }
}


class SecondOrderKey(val first: String, val second: Int) extends Ordered[SecondOrderKey] with Serializable {
  def compare(other: SecondOrderKey): Int = {

    if (this.first.equals(other.first)) {
      this.second - other.second
    } else {
      this.first.compareTo(other.first)
    }
  }
}