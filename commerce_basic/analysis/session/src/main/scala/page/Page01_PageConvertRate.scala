package page

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{PageSplitConvertRate, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    //    targetPageFlow.foreach(println)
    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    val pageIdConvert2Rdd: RDD[(String, Int)] = sessionId2GroupActionRDD.flatMap {
      case (sessionId, iters) => {
        //方法一：推荐
        val sortPageIds = iters.toList.sortWith((x, y) => {
          DateUtils.parseTime(x.action_time).getTime < DateUtils.parseTime(y.action_time).getTime
        }).map(x => x.page_id)
        //方法二：不推荐：尽量用广播变量
        /*var sortPageIds = new mutable.ArrayBuffer[Long]()
        iters.toList.sortWith((x,y) => {
          DateUtils.parseTime(x.action_time).getTime < DateUtils.parseTime(y.action_time).getTime
        }).map(x => sortPageIds += x.page_id)*/

        //错误方法：因其可能先点击页面3，在点击页面2，所以不经过时间排序直接求会有脏数据
        //        var pageIdArray = new mutable.ArrayBuffer[Long]()
        //        for (iter <- iters) {
        //          pageIdArray += iter.page_id
        //        }
        //        val sortPageIds: ArrayBuffer[Long] = pageIdArray.sortWith((x, y) => x < y)


        sortPageIds.slice(0, sortPageIds.length - 1).zip(sortPageIds.tail).map {
          case (pageIdLeft, pageIdRight) => {
            (pageIdLeft + "_" + pageIdRight, 1)
          }
        }
      }
    }

    var firstPageId: Long = targetPageFlowSplit(0).toLong

    //第一页的访问量
    var firstPageSessionCount = sessionId2ActionRDD.filter {
      case (sessionId, userVisitAction) => {
        userVisitAction.page_id == firstPageId
      }
    }.count();

    println("第一页访问量：" + firstPageSessionCount)

    val pageIdConvert2CountRDD: RDD[(String, Int)] = pageIdConvert2Rdd.reduceByKey(_ + _)

    val correctConvert2CountRDD: RDD[(String, Int)] = pageIdConvert2CountRDD.filter {
      case (pageIdConvert, count) => {
        targetPageFlow.contains(pageIdConvert)
      }
    }
    //    val resultMap = new mutable.HashMap[String, Double]()

    /*val resultRDD: Array[(String, Int)] = correctConvert2CountRDD.sortBy(x=> x._1.substring(0,1), true).collect()
    val resultMap = new mutable.HashMap[String, Double]()
    resultRDD.foreach {
      case (pageIdConvert, count) => {
        var doubleBeforeCount = firstPageSessionCount
        val rate = count.toDouble / doubleBeforeCount
        firstPageSessionCount = count
        resultMap += ((pageIdConvert, rate))
//        resultMap.put(pageIdConvert, rate)
        (pageIdConvert, rate)
      }
    }*/
    val result1 = correctConvert2CountRDD.sortBy(x => x._1.substring(0, 1), true).collect().map {
      case (pageIdConvert, count) => {
        var doubleBeforeCount = firstPageSessionCount
        println(pageIdConvert + "=" + count + "/" + firstPageSessionCount)
        val rate: Double = count.toDouble / doubleBeforeCount.toDouble
        firstPageSessionCount = count
        //        println("比率为" + pageIdConvert + "=>" + rate);
        //          resultMap += ((pageIdConvert, rate))
        (pageIdConvert, rate)
        }
    }
    val result = correctConvert2CountRDD.sortBy(x => x._1.substring(0, 1), true).map {
      case (pageIdConvert, count) => {
        var doubleBeforeCount = firstPageSessionCount
        println(pageIdConvert + "===========" + count + "/" + firstPageSessionCount)
        val rate: Double = count.toDouble / doubleBeforeCount.toDouble
        firstPageSessionCount = count
        //TODO: 此处为何多次打印
        //        println("比率为" + pageIdConvert + "=>" + rate);
        //为何没有附上值---sparkRDD中算子的内部使用外部变量只能读，不能改变值
        //          resultMap += ((pageIdConvert, rate))
        (pageIdConvert, rate)
      }
    }

    //    result.foreach {
    //      case (pageIdConvert, count) => {
    //        println(pageIdConvert + "-->" + count + "\t")
    //      }
    //    }

    val convertStr: String = result.map {
      case (pageIdConvert, rate) => {
        pageIdConvert + "=" + rate
      }
    }.collect().mkString("|")

    val pageSplitConvertRate = PageSplitConvertRate(taskUUID, convertStr);

    val pageSplitConvertRateRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate))

    import sparkSession.implicits._

    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_1227")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    //    println("数据容量" + resultMap.size);
  }
}