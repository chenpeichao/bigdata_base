package org.pcchen.chapter11;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * 自定义udf表函数即：一对多
 *
 * @author: ceek
 * @create: 2023/3/27 19:37
 */
public class UdfTest_TableFunction {
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

        //2、注册自定义表函数
        tableEnv.createTemporaryFunction("MySplit", MySplit.class);
        //3、调用udf进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user_name, url, word, assign, length  " +
                " from " +
                "   clickTable, " +
                "   LATERAL TABLE(MySplit(url)) as T(word, assign, length) " +
                " ");
        //4、转换成流打印输出
        tableEnv.toDataStream(resultTable).print("table func: ");

        env.execute();
    }

    public static class MySplit extends TableFunction<Tuple3<String, String, String>> {
        public void eval(String str) {
            for(String word : str.split("\\?")) {
                this.collect(Tuple3.of(word, "总长度为", word.length() + ""));
            }
        }
    }
}
