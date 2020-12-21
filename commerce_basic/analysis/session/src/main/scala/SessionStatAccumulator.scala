import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器，统计HashMap[String, Long]中数量
  *
  * @author ceek
  * @create 2020-12-21 10:03
  **/
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  val hashMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long];

  override def isZero: Boolean = {
    hashMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    var acc = new SessionStatAccumulator();
    acc.hashMap ++= this.hashMap
    acc
  }

  override def reset(): Unit = {
    hashMap.clear();
  }

  //TODO: 此处hashMap的封装需要重写
  override def add(v: String): Unit = {
    //    if(!hashMap.contains(v)) {
    //      hashMap += (v -> 0)
    //    }

    //    hashMap.put(v, (hashMap(v) + 1).toLong)
    hashMap.put(v, hashMap.get(v).getOrElse(0l).asInstanceOf[Long] + 1l)
  }

  //此处需要首先判定other的类型是否满足条件
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    /*other.value.foldLeft(this.hashMap) {
      case (map, (k, v)) => map.put(k, map.get(k).getOrElse(0l).asInstanceOf[Long] + v); map
    }
    other match {
      // (0 /: (1 to 100))(_+_)
      // (0 /: (1 to 100)){case (int1, int2) => int1 + int2}
      // (1 /: 100).foldLeft(0)
      // (this.countMap /: acc.countMap)
      case acc:SessionStatAccumulator => acc.hashMap.foldLeft(this.hashMap){
        case (map, (k,v)) => map += (k -> (map.getOrElse(k, 0l).asInstanceOf[Long] + v))
      }
    }*/
    other match {
      // (0 /: (1 to 100))(_+_)
      // (0 /: (1 to 100)){case (int1, int2) => int1 + int2}
      // (1 /: 100).foldLeft(0)
      // (this.countMap /: acc.countMap)
      case acc: SessionStatAccumulator => acc.value.foldLeft(this.hashMap) {
        case (map, (k, v)) => map.put(k, map.get(k).getOrElse(0l).asInstanceOf[Long] + v); map
      }
    }
  }
  override def value: mutable.HashMap[String, Long] = {
    this.hashMap
  }
}
