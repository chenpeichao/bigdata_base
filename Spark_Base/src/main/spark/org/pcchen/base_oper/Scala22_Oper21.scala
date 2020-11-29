package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-27 16:17
  **/
object Scala22_Oper21 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();

    sparkConf.setMaster("local")
    sparkConf.setAppName("Scala22_Oper21")

    val sc = new SparkContext(sparkConf);
    //初始化数据

    val listRDD1 = sc.parallelize(List(("a", "a"), ("b", 1), ("f", 1), ("a", 1)), 2);
    val listRDD2 = sc.parallelize(List(("a", "b"), ("b", "b"), ("c", 1), ("a", 5)), 4);
    //    listRDD.glom().collect().foreach(x => {x.foreach(println);println("-------");})

    val joinRDD = listRDD1.join(listRDD2);

    val cogroup: RDD[(String, (Iterable[Any], Iterable[Any]))] = listRDD1.cogroup(listRDD2)

    joinRDD.collect().foreach(println)
  }
}
