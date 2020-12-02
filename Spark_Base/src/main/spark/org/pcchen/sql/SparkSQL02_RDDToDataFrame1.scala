package main.spark.org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 通过rdd转换生成DataFrame
  */
object SparkSQL02_RDDToDataFrame1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL02_RDDToDataFrame1")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    //引入隐式转换
    import spark.implicits._;
    val listRDD: RDD[String] = spark.sparkContext.textFile("Spark_Base/in/sparksqlDataFrame.txt");

    //TODO 1、通过手动进行RDD转DataFrame
    val dataFrame: DataFrame = listRDD.map(line => {
      val arrValue: Array[String] = line.split(" ")
      (arrValue(0), arrValue(1).toInt)
    }).toDF("name", "age")

    //TODO 2、通过样例类进行RDD转DataFrame
    val dataFrameCaseClass = listRDD.map(line => {
      val kv = line.split(" ")
      People(kv(0), kv(1).toInt);
    }).toDF()

    //TODO 3、通过structType进行类型转换,不常用
    val structType: StructType = StructType {
      StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) ::
        Nil
    }
    val rowRDD: RDD[Row] = listRDD.map(line => {
      val kv = line.split(" ")
      Row(kv(0), kv(1).toInt)
    })

    val structTypeDataFrame: DataFrame = spark.createDataFrame(rowRDD, structType)

    //dataFrameCaseClass.show();
    //dataFrame.show();

    dataFrame.createOrReplaceTempView("user");
    dataFrameCaseClass.createOrReplaceTempView("people");
    structTypeDataFrame.createOrReplaceTempView("student");

    spark.sql("select * from user where age > 10").show();
    spark.sql("select * from people where age < 10").show();
    spark.sql("select * from student where age = 10").show();
  }
}

case class People(name: String, age: Int)
