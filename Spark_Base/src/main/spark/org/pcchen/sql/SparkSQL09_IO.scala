package org.pcchen.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

/**
  *
  *
  * @author ceek
  * @create 2020-12-09 14:38
  **/
object SparkSQL09_IO {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf();
    sparkConf.setAppName("SparkSQL09_IO").setMaster("local");

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    import sparkSession.implicits._;

    //json数据的读取以及parquet数据格式的落地
    /*val df: DataFrame = sparkSession.read.format("json").load("Spark_base/in/jsonFile/user.json")
    val jsonDF: DataFrame = sparkSession.read.json("Spark_base/in/jsonFile/user.json")

    df.write.mode(SaveMode.Overwrite).save("Spark_base/output/parquet");
    jsonDF.show()*/

    /*val df: DataFrame = sparkSession.read.load("Spark_Base/output/parquet")
    df.show()*/

    val dataFrameReader: DataFrameReader = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://10.10.32.62:3306/uar_xyapp_wx")
      .option("dbtable", "spark_mysql_test")
      .option("user", "uardev")
      .option("password", "uardev2017")
    val df: DataFrame = dataFrameReader.load();


    df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://10.10.32.62:3306/uar_xyapp_wx")
      .option("dbtable", "spark_mysql_test1")
      .option("user", "uardev")
      .option("password", "uardev2017")
      .save()

    val properties = new Properties();
    properties.setProperty("user", "uardev")
    properties.setProperty("password", "uardev2017")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.10.32.62:3306/uar_xyapp_wx", "spark_mysql_test2", properties)

    //    df.show()
  }
}
