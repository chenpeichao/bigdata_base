package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 双value类型的算子
  *
  * @author ceek
  * @create 2020-11-25 16:23
  **/
object Scala13_Oper12 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala13_Oper12");

    val sc = new SparkContext(sparkConf);

    //初始化数据
    val listRDD1: RDD[Int] = sc.parallelize(List(1, 2, 6));
    val listRDD2: RDD[Int] = sc.parallelize(List(6, 7, 8, 9));

    //    val reduce: Int = listRDD1.reduce(_+_)

    /*//求并集
    val unionRDD: RDD[Int] = listRDD1.union(listRDD2);
//    unionRDD.map((_,1)).reduceByKey(_+_).collect().foreach(println);
    unionRDD.glom().collect().foreach(x => {x.foreach(println); println("---------")})*/

    /*//求差集
//    val subtractRDD: RDD[Int] = listRDD1.subtract(listRDD2); //1,2
    val subtractRDD: RDD[Int] = listRDD2.subtract(listRDD1);  //7,8,9
    subtractRDD.collect().foreach(println)  */

    /*//交集
    val intersectionRDD: RDD[Int] = listRDD1.intersection(listRDD2);
    intersectionRDD.collect().foreach(println)*/

    //    //笛卡尔积
    //    val cartesianRDD: RDD[(Int, Int)] = listRDD1.cartesian(listRDD2)
    //    cartesianRDD.collect().foreach(println)

    val zipRDD: RDD[(Int, Int)] = listRDD1.zip(listRDD2);

    zipRDD.collect().foreach(println)
  }
}
