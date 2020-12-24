package session

import commons.constant.Constants
import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@Deprecated
object Session02_HourSampleSession {
  //[sessionId, fullInfo[aggrInfo+userInfo]]
  def apply(sparkSession: SparkSession, sessionId2FilterRDD: RDD[(String, String)], taskUUID: String) = {
    //计算共多少天，=>每天需要抽取的session数量
    //每天抽取的session数量/每天session的数量=每小时抽取数量/每小时数量=>每小时抽取数量(即生成这么多个不重复随机数，从小时数据中抽取session)
    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilterRDD.map {
      case (sessionId, fullInfo) => {
        val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        //获取时间为yyyy-MM-dd_mm
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
      }
    }
  }
}
