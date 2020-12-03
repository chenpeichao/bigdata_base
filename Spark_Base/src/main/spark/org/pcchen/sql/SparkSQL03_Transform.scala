package main.spark.org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL03_Transform");

    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    //当需要toDF和toDS时，需要引入上步中spark变量的隐式转换规则
    //注意：此处的spark指的是上步的spark变量
    import spark.implicits._

    //初始化原始数据RDD
    val listRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 23), (3, "wangwu", 18)))

    //rdd转换为dafaFrame：调用toDF方法即可,即指定结构即可
    val dataFrame: DataFrame = listRDD.toDF("id", "name", "age")

    //dataFrame转换为DataSet：调用as[指定类型]即可，即指定数据的类型
    val dataSet: Dataset[User] = dataFrame.as[User]

    //--------------------
    //dataSet转换为dataFrame
    val df: DataFrame = dataSet.toDF();
    //dataFrame转换为RDD(row)
    val rdd: RDD[Row] = df.rdd;

    rdd.foreach {
      case Row(id, name, age) => println(id + "=>" + name + "=>" + age)
    }

    rdd.foreach(row => {
      println(row.getInt(2))
    })

    spark.stop();
  }
}

case class User(id: BigInt, name: String, age: BigInt) {}