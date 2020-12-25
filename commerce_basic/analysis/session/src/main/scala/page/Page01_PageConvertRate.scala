package page

import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  *
  * @author ceek
  * @create 2020-12-25 14:47
  **/
object Page01_PageConvertRate {
  def apply(sparkSession: SparkSession,
            sessionId2ActionRDD: RDD[(String, UserVisitAction)],
            taskParam: JSONObject,
            taskUUID: String): Unit = {
    val targetPageFlowParam: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);

    val targetPageFlowSplit: Array[String] = targetPageFlowParam.split(",");

    val targetPageFlow: Array[String] = targetPageFlowSplit.slice(0, targetPageFlowSplit.length - 1).zip(targetPageFlowSplit.tail).map {
      case (x, y) => {
        x + "_" + y;
      }
    }
    targetPageFlow.foreach(println)
    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    val pageIdConvert2Rdd: RDD[(String, Int)] = sessionId2GroupActionRDD.flatMap {
      case (sessionId, iters) => {
        var pageIdArray = new mutable.ArrayBuffer[Long]()

        for (iter <- iters) {
          pageIdArray += iter.page_id
        }

        val sortPageIds: ArrayBuffer[Long] = pageIdArray.sortWith((x, y) => x < y)

        sortPageIds.slice(0, sortPageIds.length - 1).zip(sortPageIds.tail).map {
          case (pageIdLeft, pageIdRight) => {
            (pageIdLeft + "_" + pageIdRight, 1)
          }
        }
      }
    }

    val firstPageId: Long = targetPageFlowSplit(0).toLong

    //第一页的访问量
    var firstPageSessionCount = sessionId2ActionRDD.filter {
      case (sessionId, userVisitAction) => {
        userVisitAction.page_id == firstPageId
      }
    }.count();

    println("第一页访问量：" + firstPageSessionCount)

    val pageIdConvert2CountRDD: RDD[(String, Int)] = pageIdConvert2Rdd.reduceByKey(_ + _)

    var resultMap = new mutable.HashMap[String, Double]()

    val pp: RDD[(String, Int)] = pageIdConvert2CountRDD.filter {
      case (pageIdConvert, count) => {
        targetPageFlow.contains(pageIdConvert)
      }
    }

    val result = pp.sortByKey().map {
      case (pageIdConvert, count) => {
        var doubleBeforeCount = firstPageSessionCount
        val rate: Double = count.toDouble / doubleBeforeCount.toDouble
        firstPageSessionCount = count
        //TODO: 此处为何多次打印
        println("比率为" + pageIdConvert + "=>" + rate);
        //TODO: 为何没有附上值
        resultMap += ((pageIdConvert, rate))
        (pageIdConvert, rate)
      }
    }
    /*
        val result = pp.sortBy(x=> x._1.substring(0,1), true, 1).map {
          case (pageIdConvert, count) => {
            var doubleBeforeCount = firstPageSessionCount
            val rate = count.toDouble / doubleBeforeCount
            firstPageSessionCount = count
            println("比率为" + pageIdConvert + "=>" + rate);
            resultMap += ((pageIdConvert, rate))
          }
        }
    */




    result.foreach {
      case (pageIdConvert, count) => {
        println(pageIdConvert + "-->" + count + "\t")
      }
    }
    result.collect()
    println("数据容量" + resultMap.size);
  }
}