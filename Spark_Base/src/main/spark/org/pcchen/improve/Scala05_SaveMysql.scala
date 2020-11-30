package org.pcchen.improve

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mysql.jdbc.Connection
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author ceek
  * @create 2020-11-30 10:37
  **/
object Scala05_SaveMysql {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Scala05_SaveMysql");

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf);

    val driver = "com.mysql.jdbc.Driver";
    val dbUrl = "jdbc:mysql://10.10.32.62:3306/uar_xyapp_wx";
    val dbUsername = "uardev";
    val dbPassword = "uardev2017";

    val sql = "select username, age from spark_mysql_test where id >= ? and id <= ? ";
    val result: JdbcRDD[(String, Int)] = new JdbcRDD(
      sc,
      () => {
        //          Class.forName(driver);
        DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
      },
      sql,
      1,
      3,
      3,
      r => ((r.getString(1)), (r.getInt(2)))
    )

    println("总记录条数为：" + result.count())
    result.foreach(x => println(x._1))
    //      result.foreach(println);


    val dataRDD = sc.makeRDD(List(("掌上", 1), ("李四", 1), ("王五", 1), ("赵六", 1)));

    val ssql = "insert into spark_mysql_test(username, age) value(?,?)";
    /*dataRDD.foreach {
      case (username, age) => {
        Class.forName("com.mysql.jdbc.Driver");
        val connection: java.sql.Connection = java.sql.DriverManager.getConnection(dbUrl, dbUsername, dbPassword)
        val prepareStatement: PreparedStatement = connection.prepareStatement(ssql)
        prepareStatement.setString(1, username);
        prepareStatement.setInt(2, age);
        prepareStatement.executeUpdate();

        prepareStatement.close();
        connection.close();
      }
    }*/

    dataRDD.foreachPartition {
      dataSet => {
        val connection: java.sql.Connection = java.sql.DriverManager.getConnection(dbUrl, dbUsername, dbPassword)
        dataSet.foreach {
          case (username, age) => {
            Class.forName("com.mysql.jdbc.Driver");
            val prepareStatement: PreparedStatement = connection.prepareStatement(ssql)
            prepareStatement.setString(1, username);
            prepareStatement.setInt(2, age);
            prepareStatement.executeUpdate();

            prepareStatement.close();
            connection.close();
          }
        }
      }
    }

    //释放资源
    sc.stop();
  }
}
