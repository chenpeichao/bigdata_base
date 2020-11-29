package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
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

    val listRDD = sc.parallelize(List(("a", 1), ("b", 2), ("c", 1), ("d", 2), ("d", 2)));

    val groupRDD: RDD[(String, Iterable[Int])] = listRDD.groupByKey(2);
    groupRDD.collect().foreach(println)
    //    println(groupRDD.partitions.size)


    val wordCountRDD: RDD[(String, Int)] = groupRDD.map(x => (x._1, x._2.sum))
    println(wordCountRDD.collect().foreach(println))


    //    val resultRDD: RDD[(String, Int)] = groupRDD.mapValues(x => x.reduce(_+_))
    //
    //    val sortByRDD: RDD[(String, Int)] = resultRDD.sortBy(x=>x._1, true)
    //
    //    sortByRDD.collect().foreach(println)
  }
}
