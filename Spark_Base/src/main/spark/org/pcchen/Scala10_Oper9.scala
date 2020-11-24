package main.spark.org.pcchen

import org.apache.spark.{SparkConf, SparkContext}

object Scala10_Oper9 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("Scala10_Oper9");

    //创建上下文对象
    val sc = new SparkContext(sparkConf);

    //生成数据，并对偶数进行输出
    val listRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 2, 3));

    listRDD.distinct(5).collect().foreach(println)
  }
}
