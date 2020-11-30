package main.spark.org.pcchen.improve

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Scala09_Broadcast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("Scala09_Broadcast")
    sparkConf.setMaster("local");

    val sc = new SparkContext(sparkConf);

    val listRDD = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)));

    val list = List((1, 1), (2, 2), (3, 3), (4, 4));

    //join不能使用在非RDD的算子运算中
    //    listRDD.join(list)

    //可以使用广播变量减少数据的传输
    // 1、构造广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRDD = listRDD.map {
      case (key, value) => {
        var v2: Any = null;
        for (listValue <- broadcast.value) {
          if (key == listValue._1) {
            v2 = listValue._2;
          }
          (key, (value, v2))
        }
      }
    }
    resultRDD.foreach(println)
  }
}
