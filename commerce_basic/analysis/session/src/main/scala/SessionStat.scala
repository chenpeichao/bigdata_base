import java.util.{Date, UUID}

import Session01_StepVisitLength.{calculateStepLength, calculateVisitLength}
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{Top10Category, UserInfo, UserVisitAction}
import commons.utils._
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
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = dataRDD.map(x => (x.session_id, x))
    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    sessionId2GroupActionRDD.cache()

    //获取聚合数据里面的信息，并join用户信息[sessionId, fullInfo[aggrInfo+userInfo]]
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, sessionId2GroupActionRDD)

    //数据进行清洗并获取累加总访问量
    //注册自定义累加器
    val sessionStatAccumulator = new SessionStatAccumulator;
    sparkSession.sparkContext.register(sessionStatAccumulator, "SessionStatAccumulator");

    //获取满足过滤条件的filter数据---并进行累加器进行访问步长和访问时长的统计
    //[sessionId, fullInfo[aggrInfo+userInfo]]
    val filt2FullInfoRDD: RDD[(String, String)] = getFilteredData(jsonObject, sessionStatAccumulator, sessionId2FullInfoRDD)

    filt2FullInfoRDD.count()

    /** 需求一：计算并保存session访问时长、步长并保存 */
    //    Session01_StepVisitLength(sparkSession, sessionStatAccumulator, jsonObject, taskUUID);

    /** 需求二：随机session抽取共100，计算每小时抽取的值 */
    //    Session02_HourSampleSession(sparkSession, filt2FullInfoRDD, taskUUID);

    /** 需求三：Top10热门品类统计 */
    //将过滤后的[sessionId, userVisitAction]数据进行top10类别求解
    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(filt2FullInfoRDD).map {
      case (sessionId, (userAction, fullInfo)) => (sessionId, userAction)
    }
    val top10Categories: RDD[Top10Category] = Session03_Top10Categories(sparkSession, sessionId2FilterActionRDD, jsonObject, taskUUID)
    //    Session03_Top10Categories_Teacher(sparkSession, sessionId2FilterActionRDD, jsonObject, taskUUID)

    /** 需求四：Top10热门品类下每个品类的Top10点击session */
    //    Session04_Top10Categories2Top10SessionClickCount(sparkSession, top10Categories, sessionId2ActionRDD, taskUUID)

    //    Session04_Top10Categories2Top10SessionClickCount_Teacher(sparkSession, taskUUID, sessionId2ActionRDD, top10Categories)
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

          //填充累加器中的访问时长和访问步长
          calculateVisitLength(visitLength.toLong, sessionStatAccumulator)
          calculateStepLength(visitLength.toLong, sessionStatAccumulator)
        }

        success
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
}