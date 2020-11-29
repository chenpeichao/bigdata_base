package main.spark.org.pcchen.base_oper

import org.apache.spark.{SparkConf, SparkContext}

object Scala08_Oper7 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala07_Oper7");

    //创建上下文对象
    val sc = new SparkContext(sparkConf);

    //生成数据，并对偶数进行输出
    val listRDD = sc.makeRDD(List(1, 2, 3, 4, 5));
    val filterRDD = listRDD.filter(_ % 2 == 0);

    filterRDD.foreach(println)
  }
}
