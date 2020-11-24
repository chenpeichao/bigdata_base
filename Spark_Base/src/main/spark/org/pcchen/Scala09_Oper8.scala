package main.spark.org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala09_Oper8 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala09_Oper8");

    //创建上下文对象
    val sc = new SparkContext(sparkConf);

    //生成数据，并对偶数进行输出
    val listRDD = sc.makeRDD(List(1, 2, 3, 4, 5));

    val sampleRDD: RDD[Int] = listRDD.sample(true, 0.6)
    sampleRDD.collect().foreach(println)
  }
}
