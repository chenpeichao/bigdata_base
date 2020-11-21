package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ceek
  * @create 2020-11-19 10:16
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    //1、创建sparkconf对象
    //设定spark计算框架的运行(部署)环境
    //appid
    val sparkConf = new SparkConf();
    sparkConf.setAppName("ceek_wordCount")
    sparkConf.setMaster("local")
//    sparkConf.set("spark.master", "local");

    //2、创建spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)

    //3、读取文件，将文件内容一行一行的读取
    //  路径查找顺序默认是从项目的根目录开始
    // 如果从本地查找：file:///opt/module/spark-2.1.1/in
    //注意spark在服务器上设计操作文件时要符合hdfs的文件编码，必要时需要导入包(入lzo压缩包，并配置spark-default.sh)
    val lines: RDD[String] = sc.textFile("file://e://code/github/bigdata_base/Spark_Base/in")
    //    val lines: RDD[String] = sc.textFile("file:///opt/project/wordcount/in")

    //4、将数据每行按照空格分解成一个个单词,进行扁平化操作
    val words: RDD[String] = lines.flatMap(_.split(" "));

    //5、对分解后的数据进行结构变化，变成value为1的元组，方便后面统计
    val wordToOne: RDD[(String, Int)] = words.map((_, 1));

    //6、对转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _);

    //7、将统计后的数据进行收集后，控制台打印
    val result: Array[(String, Int)] = wordToSum.collect();

    result.foreach(println)
  }
}
