package product

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{CityAreaInfo, CityClickProduct}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  *
  * @author ceek
  * @create 2020-12-28 9:54
  **/
object ProductStat {
  def main(args: Array[String]): Unit = {
    //获取请求参数条件json串，并封装对象
    val taskParam = JSONObject.fromObject(ConfigurationManager.config.getString(Constants.TASK_PARAMS))

    //获取任务唯一标识
    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ProductStat")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sparkSession = SparkSession.builder().enableHiveSupport()
      .config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .getOrCreate()

    val areaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    val areaInfoRDD: RDD[(Long, String, String)] = sparkSession.sparkContext.makeRDD(areaInfoArray)

    //1.1、获取到有效城市维度商品点击数据RDD[CityClickProduct[cityId, productId]]
    val cityIdAndProductIdRDD: RDD[(Long, Long)] = getCityIdAndProductId(sparkSession, taskParam)

    //1.2、城市及地域对应关系[cityId, CityAreaInfo[cityId, cityName, area]]
    val cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)] = getCityAreaInfo(sparkSession)

    //2、创建临时表tmp_area_basic_info(city_id, city_name, area, pid)-方便进行数据查询
    getAreaPidBasicInfoTable(sparkSession, cityIdAndProductIdRDD, cityId2AreaInfoRDD)

    //3、创建临时表地域商品的点击数量表
    //由于要输出城市信息所以需要自定义UDAF函数进行数据输出，否则在聚合数据是无法输出group by之外的数据
    //3.1、首先定义udf函数进行cityid和cityName信息进行组装
    sparkSession.udf.register("concat_long_string", (cityId: Long, cityName: String, splitStr: String) => {
      cityId + splitStr + cityName
    })
    //3.2、定义udaf在group by中对于多行数据进行操作
    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF())
    //地域商品点击数表(包含城市信息)tmp_area_click_count（area, pid, click_count, city_info）
    getAreaProductClickCountTable(sparkSession)



    println(cityIdAndProductIdRDD.count())
    sparkSession.close();
  }

  /**
    * 进行地域商品点击行为统计(分组排序中包含group by之外的城市信息，通过自定义UDAF函数实现数据展示)
    *
    * @param sparkSession
    */
  def getAreaProductClickCountTable(sparkSession: SparkSession) = {
    //由于要输出城市信息所以需要自定义UDAF函数进行数据输出，否则在聚合数据是无法输出group by之外的数据
    //首先定义udf函数进行cityid和cityName信息进行组装
    //定义udaf在group by中对于多行数据进行操作
    val sql = "select area, pid, count(1) click_count, " +
      " group_concat_distinct(concat_long_string(city_id, city_name, '=')) city_info " +
      " from tmp_area_basic_info group by area, pid ";

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count");
  }

  /**
    * 封装城市地域商品id的RDD，每一条数据代表一个点击行为，为统计点击次数提供数据准备
    *
    * @param sparkSession
    * @param cityIdAndProductIdRDD
    * @param cityId2AreaInfoRDD
    */
  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               cityIdAndProductIdRDD: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {
    val areaPidInfoRDD: RDD[(Long, String, String, Long)] = cityIdAndProductIdRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, cityAreaInfo)) => {
        (cityId, cityAreaInfo.city_name, cityAreaInfo.area, pid)
      }
    }

    import sparkSession.implicits._

    areaPidInfoRDD.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("tmp_area_basic_info")
  }

  /**
    * 获取城市与地域对照RDD
    *
    * @param sparkSession
    * @return
    */
  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"));

    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) => {
        (cityId, CityAreaInfo(cityId, cityName, area))
      }
    }
  }

  /**
    * 获取到城市id-商品id的点击记录
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getCityIdAndProductId(sparkSession: SparkSession, taskParam: JSONObject) = {
    val sql = "select city_id, click_product_id from commerce.user_visit_action " +
      " where action_time > '" + taskParam.getString(Constants.PARAM_START_DATE) + "' and action_time < '" + taskParam.getString(Constants.PARAM_END_DATE) + "' " +
      " and click_product_id != -1 ";

    import sparkSession.implicits._

    val cityClickIdAndProductIdRDD: RDD[CityClickProduct] = sparkSession.sql(sql).as[CityClickProduct].rdd
    cityClickIdAndProductIdRDD.map {
      case cityClickProductId => {
        (cityClickProductId.city_id, cityClickProductId.click_product_id)
      }
    }
  }
}
