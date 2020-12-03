package main.spark.org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * rdd和DataSet之间相互转换
  */
object SparkSQL04_TransformRDDAndDataSet {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQL04_TransformRDDAndDataSet").setMaster("local")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    import spark.implicits._;

    var listRDD = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 23), (3, "wangwu", 18)));

    //此处最好指定数据结构和数据类型转换
    /* val ds: Dataset[(Int, String, Int)] = listRDD.toDS()
     ds.show();*/

    val dataSet: Dataset[User] = listRDD.map(row => {
      User(row._1.toInt, row._2, row._3.toInt)
    }).toDS();
    dataSet.show()

    val rdd: RDD[User] = dataSet.rdd;
    rdd.foreach(println)

    spark.stop();
  }
}