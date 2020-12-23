import commons.constant.Constants
import commons.model.Top10Category
import commons.utils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.StringOps

/**
  *
  *
  * @author ceek
  * @create 2020-12-23 14:37
  **/
object Session04_Top10Categories2Top10SessionClickCount {
  def apply(sparkSession: SparkSession, top10Categories: RDD[Top10Category], filt2FullInfoRDD: RDD[(String, String)], taskUUID: String) = {
    val categoryId2Top10Category: RDD[(Long, Top10Category)] = top10Categories.map(top10Categories => (top10Categories.categoryid, top10Categories))
    filt2FullInfoRDD.flatMap {
      case (sessionId, fullInfo) => {
        val clickCategories: StringOps = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

        //TODO: loading。。。。
        clickCategories.split(",")

        null
      }
    }
  }
}
