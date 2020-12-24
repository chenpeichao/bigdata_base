package session

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{Top10Category, UserVisitAction}
import commons.utils.StringUtils
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Session03_Top10Categories {
  def apply(sparkSession: SparkSession, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], jsonObject: JSONObject, taskUUID: String) = {
    //获取所有的商品分类(categoryId, categoryId)，以此为基准进行左连接点击、订单、支付的品类，进行二次排序后，保存top10
    val categoriesRDD: RDD[(Long, Long)] = getCatetoriesAllInfoRDD(sessionId2FilterActionRDD)

    //获取点击分类
    // 第二步：统计品类的点击次数、下单次数、付款次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilterActionRDD)

    val cid2OrderCountRDD: RDD[(Long, Long)] = getOrderCount(sessionId2FilterActionRDD)

    val cid2PayCountRDD: RDD[(Long, Long)] = getPayCount(sessionId2FilterActionRDD)

    val fullCountRDD: RDD[(Long, Top10Category)] = categoriesRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (categoryId, (categoryId2, clickCount)) => {
        val top10Categories = new Top10Category(taskUUID, categoryId, clickCount.getOrElse(0l), 0l, 0l)
        (categoryId, top10Categories);
      }
    }.leftOuterJoin(cid2OrderCountRDD).map {
      case (categoryId, (top10Categories, orderCount)) => {
        val top10Categories1 = new Top10Category(taskUUID, categoryId, top10Categories.clickCount, orderCount.getOrElse(0l), 0l)
        (categoryId, top10Categories1);
      }
    }.leftOuterJoin(cid2PayCountRDD).map {
      case (categoryId, (top10Categories, payCount)) => {
        val top10Categories2 = new Top10Category(taskUUID, categoryId, top10Categories.clickCount, top10Categories.orderCount, payCount.getOrElse(0l))
        (categoryId, top10Categories2);
      }
    }

    //对数据进行二次排序
    val sortKey2FullCountRDD: Array[(SecondSortKeyTop10Category, Long)] = fullCountRDD.map {
      case (categoryId, top10Category: Top10Category) => {
        (new SecondSortKeyTop10Category(top10Category.categoryid, top10Category.clickCount, top10Category.orderCount, top10Category.payCount), categoryId)
      }
    }.sortByKey(false).take(10);

    val resultRDD: RDD[Top10Category] = sparkSession.sparkContext.makeRDD(sortKey2FullCountRDD).map {
      case (secondSortKeyTop10Category, categoryId) => {
        Top10Category(taskUUID, categoryId, secondSortKeyTop10Category.clickCount, secondSortKeyTop10Category.orderCount, secondSortKeyTop10Category.payCount)
      }
    }

    import sparkSession.implicits._
    resultRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_1222")
      .mode(SaveMode.Append)
      .save();

    resultRDD
  }

  //获取点击的品类统计[categoryId, clickCount]
  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD.map {
      case (sessionId, userVisitAction: UserVisitAction) => {
        (userVisitAction.click_category_id, 1l)
      }
    }.reduceByKey(_ + _)
  }

  //获取订单的品类统计[categoryId, orderCount]
  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) => {
        var categories = new ArrayBuffer[Long]();
        if (StringUtils.isNotEmpty(userVisitAction.order_category_ids)) {
          val categoriesArray: Array[String] = userVisitAction.order_category_ids.split(",")
          categoriesArray.foreach(x => categories += x.toLong)
        }
        categories
      }
    }.map((_, 1l)).reduceByKey(_ + _)
  }

  //获取支付的品类统计[categoryId, payCount]
  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) => {
        var categories = new ArrayBuffer[Long]();
        if (StringUtils.isNotEmpty(userVisitAction.pay_category_ids)) {
          val categoriesArray: Array[String] = userVisitAction.pay_category_ids.split(",")
          categoriesArray.foreach(x => categories += x.toLong)
        }
        categories
      }
    }.map((_, 1l)).reduceByKey(_ + _)
  }

  //获取所有品类的key，value元素[categoryId, categoryId]
  def getCatetoriesAllInfoRDD(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val categoriesRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sessionId, userVisitAction) => {
        var categories = new ArrayBuffer[(Long, Long)]();

        if (-1l != userVisitAction.click_category_id) {
          categories += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
        } else if (StringUtils.isNotEmpty(userVisitAction.pay_category_ids)) {
          userVisitAction.pay_category_ids.split(",").foreach {
            case (category) => {
              categories += ((category.toLong, category.toLong))
            }
          }
        } else if (StringUtils.isNotEmpty(userVisitAction.order_category_ids)) {
          userVisitAction.order_category_ids.split(",").foreach {
            case (category) => {
              categories += ((category.toLong, category.toLong))
            }
          }
        }
        categories
      }
    }

    categoriesRDD.distinct()
  }
}
