package main.spark.org.pcchen

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala24_Oper23 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("Scala24_Oper23");
    sparkConf.setMaster("local[3]");

    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[Int] = sc.parallelize(1 to 20);

    listRDD.glom().collect().foreach(x => {
      x.foreach(println); println("------")
    })

    listRDD.saveAsTextFile("Spark_Base/output_text")
    listRDD.saveAsObjectFile("Spark_Base/output_object")
  }
}
