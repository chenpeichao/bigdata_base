package org.pcchen.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义udf中的标量函数即：1对1
 *
 * @author: ceek
 * @create: 2023/3/27 19:26
 */
public class UdfTest_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1、在创建表的DDL语句中直接定义时间语义
        String createDDL = " CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                " ) WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'Flink_Base\\input\\clicks.txt', " +
                " 'format' = 'csv' " +
                " )";
        tableEnv.executeSql(createDDL);

        //2、注册自定义标量函数
        tableEnv.createTemporaryFunction("myHashFunc", MyHashFunction.class);
        //3、调用udf进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user_name, myHashFunc(user_name) from clickTable");
        //4、转换成流打印输出
        tableEnv.toDataStream(resultTable).print("scalar func: ");

        env.execute();
    }

    //自定义scalarfunction
    public static class MyHashFunction extends ScalarFunction {
        public int eval(String url) {
            return url.hashCode();
        }
    }
}
