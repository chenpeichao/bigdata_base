package org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ceek
  * @create 2020-11-27 9:29
  **/
object Scala19_Oper18 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();

    sparkConf.setMaster("local")
    sparkConf.setAppName("Scala19_Oper18")

    val sc = new SparkContext(sparkConf);
    //初始化数据

    val listRDD = sc.parallelize(List(("a", 10), ("b", 10), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2);
    listRDD.glom().collect().foreach(x => {
      x.foreach(println); println("-------");
    })

    //注意此处是否执行第三个参数与rdd是否只有唯一分区有关
    //由于第二个参数的前面一个key是第一个参数的返回值，不知道类型，所以第二个参数要手动知名tuple的类型
    val combineByKeyRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey(x => (x, 1), (y: (Int, Int), v) => (y._1 + v, y._2 + 1), (res: (Int, Int), p: (Int, Int)) => (res._1, res._1))

    val resRDD: RDD[(String, Int)] = combineByKeyRDD.map(x => (x._1, x._2._1 / x._2._2));

    resRDD.collect().foreach(println)

    //combineByKey效果相同

    //    val resRDD: RDD[(String, Int)] = listRDD.groupBy(x => x._1).map(x => {
    //      val iterValues = x._2;
    //      var sum = 0;
    //      for (i <- iterValues) {
    //        sum += i._2;
    //      }
    //      (x._1, sum / iterValues.size)
    //    })
    //    resRDD.collect().foreach(println)

    val combineByKeyRDD1 = listRDD.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (res1: (Int, Int), res2: (Int, Int)) => (res1._1 + res2._1, res1._2 + res2._2))
    val resRDD1: RDD[(String, Int)] = combineByKeyRDD1.map(x => (x._1, x._2._1 / x._2._2));

    resRDD1.collect().foreach(println)
  }
}
