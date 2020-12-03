package main.spark.org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQL05_UDF").setMaster("local")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    import spark.implicits._;

    var listRDD = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 23), (3, "wangwu", 18)));

    //此处最好指定数据结构和数据类型转换
    /* val ds: Dataset[(Int, String, Int)] = listRDD.toDS()
     ds.show();*/

    spark.udf.register("addOne", (x: Int) => x + 1)

    val dataSet: Dataset[User] = listRDD.map(row => {
      User(row._1.toInt, row._2, row._3.toInt)
    }).toDS();

    dataSet.createOrReplaceTempView("user");

    spark.sql("select addOne(id) from user").show()

    spark.stop();
  }
}
