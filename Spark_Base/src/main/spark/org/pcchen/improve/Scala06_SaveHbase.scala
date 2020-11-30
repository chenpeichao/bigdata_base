package org.pcchen.improve

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-30 14:39
  **/
object Scala06_SaveHbase {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Scala06_SaveHbase");

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf);

    //    val conf: Configuration = HBaseConfiguration.create();
    //
    //    conf.set(TableInputFormat.INPUT_TABLE, "student");

    //TODO hbase数据查询
    /*val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.foreach {
      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells();
        for(cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }*/

    /*//TODO hbase数据写入
    val dataRDD: RDD[(String, String)] = sc.parallelize(List(("1001", "张三"), ("1002", "李四"), ("1003", "王五")));

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));

        (new ImmutableBytesWritable((Bytes.toBytes(rowkey))), put)
      }
    }

    val jobConf = new JobConf(conf);
    jobConf.setOutputFormat(classOf[TableOutputFormat]);
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student");

    putRDD.saveAsHadoopDataset(jobConf);*/

    sc.stop();
  }
}
