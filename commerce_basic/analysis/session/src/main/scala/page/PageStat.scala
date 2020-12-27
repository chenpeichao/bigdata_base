package page

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author ceek
  * @create 2020-12-24 15:10
  **/
object PageStat {

  def main(args: Array[String]): Unit = {
    val taskParam = JSONObject.fromObject(ConfigurationManager.config.getString(Constants.TASK_PARAMS));
    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString;

    val sparkConf = new SparkConf().setAppName("PageStat").setMaster("local[*]")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport()
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .getOrCreate();

    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getActionRDD(sparkSession, taskParam)

    //    sessionId2ActionRDD.foreach(println)

    /** 需求五：页面单跳转化率 */
    Page01_PageConvertRate(sparkSession, sessionId2ActionRDD, taskParam, taskUUID);
    //    Page01_PageConvertRate_Teacher(sparkSession, sessionId2ActionRDD, taskParam, taskUUID)
  }

  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from commerce.user_visit_action where date > '" + startDate + "' and date < '" + endDate + "'";

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].map(uservisitAction => {
      (uservisitAction.session_id, uservisitAction)
    }).rdd
  }
}
