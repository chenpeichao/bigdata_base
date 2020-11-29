package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala07_Oper6 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Scala07_6", new SparkConf());

    //生成数据，按照指定的规则进行分组
    val listRDD = sc.makeRDD(List(1, 2, 3, 4, 5));

    //分组后的数据形成了对偶元祖(K->V)，K表示分组的key，V表示分组的数据集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x);

    groupByRDD.collect().foreach(println)
    //    groupByRDD.collect().foreach(x => println((x._1, x._2.sum)))

    //    val resRDD: RDD[(Int, Int)] = groupByRDD.map(x => (x._1, x._2.sum))
    //    println(resRDD.collect().foreach(println))

    //    val list = List(1,2,3,4,5,2)
    //    val intToInts: Map[Int, List[Int]] = list.groupBy(x => x);
    //    val tuples: List[(Int, Int)] = intToInts.toList.map(x => (x._1, x._2.sum))
    //    println(tuples)
  }
}
