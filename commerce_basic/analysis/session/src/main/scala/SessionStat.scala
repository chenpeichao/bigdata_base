import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  *
  * @author ceek
  * @create 2020-12-18 15:12
  **/
object SessionStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SessionStat").setMaster("local[*]")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    //设置hive远程地址
    val sparkSession = SparkSession.builder().config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .enableHiveSupport()
      .getOrCreate();

    //设置筛选条件对应的jsonObject
    val jsonObject = JSONObject.fromObject(ConfigurationManager.config.getString(Constants.TASK_PARAMS))

    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString;

    val dataRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, jsonObject)
    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = dataRDD.map(x => (x.session_id, x)).groupByKey()

    sessionId2GroupActionRDD.cache()

    val userId2AggrInfoRDD: RDD[(Long, String)] = getSessionFullInfo(sessionId2GroupActionRDD)

    userId2AggrInfoRDD.take(10).foreach(println)
  }

  //对每个sessionid([sessionId, iter[UserVisitAction]])数据进行数据整合=>[userId, aggreInfo]
  def getSessionFullInfo(parm: RDD[(String, Iterable[UserVisitAction])]) = {
    parm.map {
      case (sessionId, iter) => {
        var userId = -1l;
        var startTime: Date = null;
        var endTime: Date = null;
        var stepLength = 0;

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        iter.foreach(userAction => {
          stepLength += 1; //访问步长
          if (userId == -1) { //用户id
            userId = userAction.user_id
          }

          //对于访问初始、截止时间的封装
          val actionTime = DateUtils.parseTime(userAction.action_time)
          if (startTime == null || actionTime.before(startTime)) {
            startTime = actionTime;
          }
          if (endTime == null || actionTime.after(endTime)) {
            endTime = actionTime;
          }

          //对于查询关键字的封装
          val searchKeyword = userAction.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword).append(",")
          }

          val clickCategoryId = userAction.click_category_id;
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId).append(",")
          }
        })
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000 //访问时长second

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
      }
    }
  }

  /**
    * 获取指定时间段查询条件的数据
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, "startDate");
    val endDate = ParamUtils.getParam(taskParam, "endDate");

    val sql = "select * from commerce.user_visit_action where action_time >= '" + startDate + "' and action_time <= '" + endDate + "'";

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd;
  }
}