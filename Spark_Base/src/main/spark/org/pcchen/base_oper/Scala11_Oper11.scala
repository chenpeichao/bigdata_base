package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-25 15:56
  **/
object Scala11_Oper11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Scala11_Oper10")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3);

    //使用sortBy中方法先对数据进行处理，处理后的数据比较结果进行排序，默认为正序
    val sortByRDD = listRDD.sortBy(x => x, false)
    //    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x)
    //    groupByRDD.map(x => (x._1, x._2))
    listRDD.map(x => x);

    sortByRDD.collect().foreach(println);
  }
}
