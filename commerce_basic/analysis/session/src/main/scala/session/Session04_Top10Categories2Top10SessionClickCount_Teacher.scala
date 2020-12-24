package session

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{Top10Category, Top10Session, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  *
  *
  * @author ceek
  * @create 2020-12-24 11:42
  **/
object Session04_Top10Categories2Top10SessionClickCount_Teacher {
  def apply(sparkSession: SparkSession,
            taskUUID: String,
            sessionId2SourceActionRDD: RDD[(String, UserVisitAction)],
            top10Categories: RDD[Top10Category]): Unit = {
    //     第一步：过滤出所有点击过Top10品类的action
    //     1： join
    val cid2CountInfoRDD = top10Categories.map {
      case (top10Category) =>
        //        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        (top10Category.categoryid, top10Category.categoryid)
    }

    val cid2ActionRDD = sessionId2SourceActionRDD.map {
      case (sessionId, action) =>
        val cid = action.click_category_id
        (cid, action)
    }

    val sessionId2ActionRDDB = cid2CountInfoRDD.join(cid2ActionRDD).map {
      case (cid, (categoryId, action)) =>
        val sid = action.session_id
        (sid, action)
    }

    // 2：使用filter
    // cidArray: Array[Long] 包含了Top10热门品类ID
    //    val cidArray = top10Categories.map{
    //      case (sortKey, countInfo) =>
    //        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
    //        cid
    //    }

    val cidArray = top10Categories.map {
      case (top10Category) => {
        top10Category.categoryid
      }
    }.collect()

    // 所有符合过滤条件的，并且点击过Top10热门品类的action
    val sessionId2ActionRDD = sessionId2ActionRDDB.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }

    // 按照sessionId进行聚合操作
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // cid2SessionCountRDD: RDD[(cid, sessionCount)]
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>

        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction) {
          val cid = action.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        // 记录了一个session对于它所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }

    // cid2GroupRDD: RDD[(cid, iterableSessionCount)]
    // cid2GroupRDD每一条数据都是一个categoryid和它对应的所有点击过它的session对它的点击次数
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    // top10SessionRDD: RDD[Top10Session]
    val top10SessionRDD = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // flase: item2放在前面
        // item: sessionCount   String   "sessionId=count"
        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        val top10Session: List[Top10Session] = sortList.map {
          // item : sessionCount   String   "sessionId=count"
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }

        top10Session
    }

    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()

  }
}
