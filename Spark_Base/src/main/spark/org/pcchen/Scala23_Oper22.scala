package main.spark.org.pcchen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala23_Oper22 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("Scala23_Oper22");
    sparkConf.setMaster("local[3]");

    val sc = new SparkContext(sparkConf);

    val listRDD: RDD[Int] = sc.parallelize(1 to 20);

    listRDD.glom().collect().foreach(x => {
      x.foreach(println); println("------")
    })

    /*val sum: Int = listRDD.reduce(_ + _)
    val sumTuple: (Int, Int) = listRDD.map((_, 1)).reduce((x, y) => (x._1+y._1, x._2+y._2))
    println(sum);
    println(sumTuple);

    //返回数据集元素个数
    println(listRDD.count())

    listRDD.first()
    listRDD.map((_,1)).first()

    listRDD.map((_,1)).takeOrdered(3).foreach(println)*/

    println(listRDD.map((_, 1)).aggregate(1)((v, acc: (Int, Int)) => acc._1, _ + _)) //每个分区最后一个key，累加后，再加常量6+13+20+1=40
    //    println(listRDD.map((_, 1)).aggregate(1)((v, acc:(Int, Int))=>acc._2 + v , _ + _))  //24
    //    println(listRDD.map((_, 1)).fold((1, 2))((x, y) => (x._1+y._1, x._2+y._2))) //(214,28)
    //
    //    println(listRDD.sum())
  }
}
