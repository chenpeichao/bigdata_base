package product

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{AreaTop3Product, CityAreaInfo, CityClickProduct}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    //地域商品点击数表(包含城市信息)tmp_area_click_count（area, pid, click_count, city_info）
    getAreaProductClickCountTable(sparkSession)

    //4、关联商品表，进行商品名称及商品自营信息关联(tmp_area_count_product_info[area, click_count,pid,city_info,product_name,product_status])
    sparkSession.udf.register("get_json_field", (jsonStr: String, field: String) => {
      JSONObject.fromObject(jsonStr).getString(field)
    })
    getAreaProductClickCountInfo(sparkSession)

    //5、分组排序-得到地域topN点击量的商品-通过开窗函数tmp_top3_product(area_level, area, click_count, pid, city_info, product_name, product_status)
    getTop3Product(sparkSession)

    //数据保存
    val areaTop3ProductRDD: RDD[AreaTop3Product] = sparkSession.sql("select * from tmp_top3_product").rdd.map {
      case row => {
        AreaTop3Product(taskUUID,
          row.getAs[String]("area"),
          row.getAs[String]("area_level"),
          row.getAs[Long]("pid"),
          row.getAs[String]("city_info"),
          row.getAs[Long]("click_count"),
          row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
      }
    }
    import sparkSession.implicits._;

    areaTop3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product_1229")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()



    println(cityIdAndProductIdRDD.count())
    sparkSession.close();
  }

  /**
    * 分组内排序(使用row_number() over开窗函数)以及结合case when then...else...end
    *
    * @param sparkSession
    */
  def getTop3Product(sparkSession: SparkSession) = {
    //row_number() over([ partition] order by...)[ partition内]连续递增顺序
    //rank()  over([ partition...)[ partition内]跳跃排序 12244678
    //dense_rank()  over([ partition...)[ partition内]不跳跃排序1223456
    val sql = "select CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B_Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C_Level' " +
      "ELSE 'D_Level' " +
      "END area_level " +
      " ,area, click_count, pid, city_info, product_name, product_status from (" +
      " select area, click_count, pid, city_info, product_name, product_status" +
      ", row_number() over (partition by area order by click_count desc) as rank " +
      " from tmp_area_count_product_info" +
      ") t where t.rank < 11"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_top3_product")
  }

  /**
    * 关联地域商品点击表及商品表，进行商品名称的整合
    *
    * @param sparkSession
    */
  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    val sql = "select tacc.area, tacc.click_count, tacc.pid, tacc.city_info " +
      ", pi.product_name" +
      //      ", case when get_json_field(pi.extend_info, 'product_status')='0' then 'self' else 'Third Party' end area_level" +
      //      ", if(get_json_field(pi.extend_info, 'product_status') == 1, 'self', 'Third Party') area_level_2" +
      ", if(get_json_field(pi.extend_info, 'product_status') = '0', 'self', 'Third Party') product_status" +
      //      ", get_json_field(pi.extend_info, 'product_status') product_status" +
      " from tmp_area_click_count tacc" +
      " join commerce.product_info pi on tacc.pid = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")

    //    sparkSession.sql("select * from tmp_area_count_product_info ").show()
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
