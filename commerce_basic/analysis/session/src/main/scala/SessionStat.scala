import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{SessionAggrStat, UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

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

    //获取聚合数据里面的信息，并join用户信息[sessionId, fullInfo[aggrInfo+userInfo]]
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, sessionId2GroupActionRDD)

    //数据进行清洗并获取累加总访问量
    //注册自定义累加器
    val sessionStatAccumulator = new SessionStatAccumulator;
    sparkSession.sparkContext.register(sessionStatAccumulator, "SessionStatAccumulator");

    //获取满足过滤条件的filter数据---并进行累加器进行访问步长和访问时长的统计
    val filt2FullInfoRDD: RDD[(String, String)] = getFilteredData(jsonObject, sessionStatAccumulator, sessionId2FullInfoRDD)

    //数据打印
    //    filt2FullInfoRDD.take(10).foreach(println)
    filt2FullInfoRDD.count()

    saveFinalData(sparkSession, taskUUID, sessionStatAccumulator.value)
  }

  def saveFinalData(sparkSession: SparkSession,
                    taskUUID: String,
                    value: mutable.HashMap[String, Long]): Unit = {
    //获取总的session个数
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1l).asInstanceOf[Long].toDouble

    // 不同范围访问时长的session个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0l)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0l)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0l)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0l)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0l)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0l)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0l)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0l)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0l)

    // 不同访问步长的session个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0l)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0l)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0l)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0l)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0l)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0l)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val dataRDD = sparkSession.sparkContext.makeRDD(Array(stat));

    import sparkSession.implicits._;

    dataRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_ration_1221")
      .mode(SaveMode.Append)
      .save();
  }


  /**
    * 对有效数据进行过滤，并在累加器中进行步长和时长的统计
    *
    * @param taskParam
    * @param sessionStatAccumulator
    * @param sessionId2FullInfoRDD
    * @return
    */
  def getFilteredData(taskParam: JSONObject,
                      sessionStatAccumulator: SessionStatAccumulator,
                      sessionId2FullInfoRDD: RDD[(String, String)]) = {
    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true;

        val startAgeFilter = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        val endAgeFilter = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        val professionalFilter = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        val citiesFilter = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        val sexFilter = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        val keywordsFilter = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        val categoryIdsFilter = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

        val age: Int = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_AGE).toInt;
        val professionals: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PROFESSIONAL);
        val cities: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CITY);
        val sex: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEX);
        val keywords: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
        val categoryIds: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID);

        if (StringUtils.isNotEmpty(startAgeFilter) && age < taskParam.get(Constants.PARAM_START_AGE).asInstanceOf[Int]) {
          success = false;
        } else if (StringUtils.isNotEmpty(endAgeFilter) && age > taskParam.get(Constants.PARAM_END_AGE).asInstanceOf[Int]) {
          success = false;
        } else if (StringUtils.isNotEmpty(professionalFilter) && !ValidUtils.inCheck(professionals, professionalFilter)) {
          success = false;
        } else if (StringUtils.isNotEmpty(citiesFilter) && !ValidUtils.inCheck(cities, citiesFilter)) {
          success = false;
        } else if (StringUtils.isNotEmpty(sexFilter) && !sexFilter.equals(sex)) {
          success = false;
        } else if (StringUtils.isNotEmpty(keywordsFilter) && !ValidUtils.inCheck(keywords, keywordsFilter)) {
          success = false;
        } else if (StringUtils.isNotEmpty(categoryIdsFilter) && !ValidUtils.inCheck(categoryIds, categoryIdsFilter)) {
          success = false;
        }

        //当符合条件保留的数据进行累加器计算访问步长和访问时长
        if (success) {
          sessionStatAccumulator.add(Constants.SESSION_COUNT);

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH);

          calculateVisitLength(visitLength.toLong, sessionStatAccumulator)
          calculateStepLength(visitLength.toLong, sessionStatAccumulator)
        }

        success
      }
    }
  }

  //向累加器中添加访问步长的hashMap的key
  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionStatAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  //向累加器中添加访问时长的hashMap的key
  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionStatAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  //对每个sessionid([sessionId, iter[UserVisitAction]])数据进行数据整合=>[userId, aggreInfo]
  //获取到所有的用户信息userInfoRDD[userid, userInfo]]
  //两个数据进行join获取到组合信息=>[sessionId, fullInfo[aggInfo+userInfo]
  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    //根据用户sessionId2GroupRDD[session, Iter[UserVisitAction]]得到userId2AggInfo[userId, aggInfoStr]
    val userId2AggInfoRDD = sessionId2GroupRDD.map {
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

    //获取到用户
    val sql = "select * from commerce.user_info";

    import sparkSession.implicits._;

    val userInfoRdd: RDD[(Long, UserInfo)] = sparkSession.sql(sql).as[UserInfo].rdd.map(userInfo => (userInfo.user_id, userInfo))

    val sessionId2FullInfoRDD = userId2AggInfoRDD.join(userInfoRdd).map {
      case (userId, (aggrInfo, userInfo)) => {
        val age = userInfo.age;
        val professional = userInfo.professional;
        val sex = userInfo.sex;
        val city = userInfo.city;

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age +
          "|" + Constants.FIELD_PROFESSIONAL + "=" + professional +
          "|" + Constants.FIELD_SEX + "=" + sex +
          "|" + Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID);

        (sessionId, fullInfo);
      }
    }

    sessionId2FullInfoRDD
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