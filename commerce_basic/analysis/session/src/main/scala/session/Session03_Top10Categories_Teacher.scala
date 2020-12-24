package session

import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.StringUtils
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *
  *
  * @author ceek
  * @create 2020-12-22 15:42
  **/
object Session03_Top10Categories_Teacher {
  def apply(sparkSession: SparkSession, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], jsonObject: JSONObject, taskUUID: String): Unit = {
    // 第一步：获取所有发生过点击、下单、付款的品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap {
      case (sid, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        // 点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }

    cid2CidRDD = cid2CidRDD.distinct()

    //获取点击分类
    // 第二步：统计品类的点击次数、下单次数、付款次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilterActionRDD)

    val cid2OrderCountRDD: RDD[(Long, Long)] = getOrderCount(sessionId2FilterActionRDD)

    val cid2PayCountRDD: RDD[(Long, Long)] = getPayCount(sessionId2FilterActionRDD)


    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
    }

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }

    // 实现自定义二次排序key
    val sortKey2FullCountRDD = cid2PayInfoRDD.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SecondSortKeyTop10Category(cid, clickCount, orderCount, payCount)

        (sortKey, countInfo)
    }

    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)

    top10CategoryArray.foreach(println)
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    //    val clickFilterRDD = sessionId2FilterActionRDD.filter{
    //      case (sessionId, action) => action.click_category_id != -1L
    //    }
    //  先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)

    // 进行格式转换，为reduceByKey做准备
    val clickNumRDD = clickFilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }

    clickNumRDD.reduceByKey(_ + _)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD = orderFilterRDD.flatMap {
      // action.order_category_ids.split(","): Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong, 1L)
      // 先将我们的字符串拆分成字符串数组，然后使用map转化数组中的每个元素，
      // 原来我们的每一个元素都是一个string，现在转化为（long, 1L）
      case (sessionId, action) => action.order_category_ids.split(",").
        map(item => (item.toLong, 1L))
    }

    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap {
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_ + _)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
    }

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }

    cid2PayInfoRDD
  }
}
