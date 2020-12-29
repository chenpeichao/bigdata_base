package product

import commons.utils.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/**
  * 自定义UDAF函数进行城市信息统计
  * 输入多行城市数据，输出一行城市数据，中间用，号分隔
  *
  * @author ceek
  * @create 2020-12-28 14:46
  **/
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("cityInfo", StringType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("bufferCityInfo", StringType)
  }

  override def dataType: DataType = {
    StringType
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var cityInfos: String = buffer.getString(0)

    if (!cityInfos.contains(input.getString(0))) {
      if (StringUtils.isNotEmpty(cityInfos)) {
        cityInfos = cityInfos + "," + input.get(0).toString
      } else {
        cityInfos = input.get(0).toString
      }
      buffer.update(0, cityInfos)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var cityInfos: String = buffer1.getString(0)
    val toPut: String = buffer2.getString(0)
    for (cityInfo <- toPut.split(",")) {
      if (!cityInfos.contains(cityInfo)) {
        if (StringUtils.isNotEmpty(cityInfos)) {
          cityInfos = cityInfos + "," + cityInfo
        } else {
          cityInfos = cityInfo
        }
      }
      buffer1.update(0, cityInfos)
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
