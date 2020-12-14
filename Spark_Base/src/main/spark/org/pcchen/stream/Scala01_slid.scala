package org.pcchen.stream

/**
  *
  *
  * @author ceek
  * @create 2020-12-14 15:28
  **/
object Scala01_slid {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8);
    //欢动窗口
    val sliding: Iterator[List[Int]] = list.sliding(3)

    sliding.foreach(x => {
      println(x)
    })
  }
}
