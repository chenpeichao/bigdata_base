package session

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{Top10Category, Top10Session, UserVisitAction}
import commons.utils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable._

/**
  * 保存Top10品类下Top10的用户访问session的信息
  *
  * @author ceek
  * @create 2020-12-23 14:37
  **/
object Session04_Top10Categories2Top10SessionClickCount {
  def apply(sparkSession: SparkSession, top10Categories: RDD[Top10Category],
            sessionId2SourceActionRDD: RDD[(String, UserVisitAction)],
            taskUUID: String) = {
    val categoryId2Top10CategoryRDD: RDD[(Long, Top10Category)] = top10Categories.map(top10Categories => (top10Categories.categoryid, top10Categories))
    val categoryId2SessionIdRDD: RDD[(Long, String)] = sessionId2SourceActionRDD.map {
      case (sessionId, action) =>
        val cid = action.click_category_id
        (action.click_category_id, sessionId)
    }
    /*
        val categoryId2SessionIdRDD : RDD[(Long, String)] = filt2FullInfoRDD.filter(x => {StringUtils.isNotEmpty(StringUtils.getFieldFromConcatString(x._2, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))}).flatMap {
          case (sessionId, fullInfo) => {
            val clickCategories: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            //TODO: loading。。。。
            clickCategories.split(",").map(x => (x.toLong, sessionId))
          }
        }
    */

    val resultRDD: RDD[(Long, (String, Int))] = categoryId2Top10CategoryRDD.leftOuterJoin(categoryId2SessionIdRDD).map {
      case (categoryId1, (categoryId2, sessionId)) => {
        ((categoryId1, sessionId), 1)
      }
    }.reduceByKey(_ + _).map {
      case ((categoryId, sessionId), count) => {
        (categoryId, (sessionId, count))
      }
    }.groupByKey().flatMap {
      case (categorId, iterots) => {
        val keyValue = new ArrayBuffer[(String, Int)]()

        for (iter <- iterots) {
          val sessionId: String = iter._1.getOrElse("no")
          if (StringUtils.isNotEmpty(sessionId)) {
            keyValue += ((sessionId, iter._2.toInt))
          }
        }

        val tuples: ArrayBuffer[(String, Int)] = keyValue.sortWith {
          case (x, y) => {
            x._2 > y._2
          }
        }.take(10)

        tuples.map(tuple => {
          (categorId, (tuple._1, tuple._2))
        })
      }
    }
    resultRDD.count()
    //    val key: RDD[(Long, (String, Int))] = resultRDD.sortByKey(false)
    //
    //    key.foreach(println)

    val jdbcRDD: RDD[Top10Session] = resultRDD.map {
      case (categoryId, (sessionid, count)) => {
        Top10Session(taskUUID, categoryId, sessionid, count);
      }
    }


    import sparkSession.implicits._;

    jdbcRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_1224")
      .mode(SaveMode.Append)
      .save();
  }
}
