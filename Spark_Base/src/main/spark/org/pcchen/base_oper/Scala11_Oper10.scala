package main.spark.org.pcchen.base_oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ceek
  * @create 2020-11-25 14:47
  **/
object Scala11_Oper10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Scala11_Oper10")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3);

    //输出位1,2  3,4,5  6,7,8
    listRDD.glom().collect().foreach(x => {
      x.foreach(println);
      println("--------")
    })

    val size: Int = listRDD.partitions.size
    println(s"分区前的总分区数为$size")

    //当shuffle为false时，老分区数据往最后一个新分区后面追加
    //当shuffle为true时，会打乱分区重新划分
    val coalesceRDD: RDD[Int] = listRDD.coalesce(2, true);
    //底层调用的是coalesce
    //    listRDD.repartition(3)

    printf("分区后的总分区数为%d\t", coalesceRDD.partitions.size)
    coalesceRDD.glom().collect().foreach(x => {
      x.foreach(println);
      println("======");
    })
  }
}
