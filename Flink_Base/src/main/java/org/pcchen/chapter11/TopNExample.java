package org.pcchen.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 求topN的简单版
 *
 * @author: ceek
 * @create: 2023/3/27 18:40
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        //1、普通topN，选取当前所有用户中浏览量最大的2个
        Table topNResultTable = tableEnv.sqlQuery("SELECT user_name, cnt, row_num " +
                "FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (" +
                "      ORDER BY cnt DESC" +
                "   ) AS row_num " +
                "   FROM (SELECT user_name, COUNT(url) AS cnt FROM clickTable GROUP BY user_name)" +
                ") WHERE row_num <= 2");


        //2、窗口topN，统计一段时间内的(前两名)活跃用户
        //2.1、子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQueryStr = "SELECT user_name, COUNT(url) AS cnt, window_start, window_end" +
                " from TABLE(" +
                "   TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)) " +
                " GROUP BY user_name, window_start, window_end";
        //2.2、定义topN的外部查询；由于是窗口查询所以需要在子查询中得到window_start和window_end的信息
        Table windowTopNResultTable = tableEnv.sqlQuery("SELECT user_name, cnt, row_num, window_start, window_end " +
                "FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (" +
                "      PARTITION BY window_start, window_end" +
                "      ORDER BY cnt DESC" +
                "   ) AS row_num " +
                "   FROM (" + subQueryStr + ")" +
                ") WHERE row_num <= 2");

//        tableEnv.toChangelogStream(topNResultTable).print("topN: ");
        tableEnv.toChangelogStream(windowTopNResultTable).print("windowTopN: ");

        env.execute();
    }
}