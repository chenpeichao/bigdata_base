package org.pcchen;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 通过sql进行cdc的监控
 *
 * @author: ceek
 * @create: 2023/3/29 15:57
 */
public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {
        //1、获得执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2、创建DDL语句
        tableEnv.executeSql("CREATE TABLE mysql_binlog ( " +
                " id STRING NOT NULL, " +
                " tm_name STRING, " +
                " logo_url STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'hadoop101', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = 'root_123', " +
                " 'database-name' = 'gmall_flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //3、查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");
        //4、将动态表转换成流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        //5、进行打印
        retractStream.print();
        env.execute("FlinkCDCWithSQL");
    }
}
