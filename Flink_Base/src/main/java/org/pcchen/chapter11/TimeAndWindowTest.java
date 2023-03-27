package org.pcchen.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 时间以及窗口的相关操作
 *
 * @author: ceek
 * @create: 2023/3/25 11:21
 */
public class TimeAndWindowTest {
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
        /*String createDDL1 = " CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                " ) WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'Flink_Base\\input\\clicks.txt', " +
                " 'format' = 'csv' " +
                " )";*/

        tableEnv.executeSql(createDDL);

        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.getTimestamp();
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream,
                $("name"), $("urls"), $("timestamp").as("ts"), $("et").rowtime());;

//        clickTable.printSchema();
        //1、聚合查询转换
        //1.1、分组聚合
        Table aggTable = tableEnv.sqlQuery("select user_name, count(1) from clickTable group by user_name");

        //1.2、分组窗口聚合--老版本
        Table groupWindowResultTable = tableEnv.sqlQuery(" select " +
                " user_name, count(1) AS cnt, " +
                " TUMBLE_END(et, INTERVAL '10' SECOND) AS endT " +
                " from clickTable " +
                " group by " +
                "       user_name, " +
                "       TUMBLE(et, INTERVAL '10' SECOND) ");

        //2、窗口聚合
        //2.1 滚动窗口-新版本，于1.2功能相同
        Table tumbleWindowResultTable = tableEnv.sqlQuery(" select user_name, count(1) as cnt, " +
                " window_start startT, window_end as endT " +
                " from TABLE( " +
                "   TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                " ) " +
                " group by user_name, window_end, window_start ");
        //2.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery(" select user_name, count(1) as cnt, " +
                " window_start as startT, window_end as endT " +
                " from TABLE(" +
                "   HOP(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
                " ) " +
                " group by user_name, window_start, window_end ");
        //2.3 累计窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery(" select user_name, count(1) as cnt, " +
                " window_start as startT, window_end as endT " +
                " from TABLE(" +
                "   cumulate(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
                " ) " +
                " group by user_name, window_start, window_end ");
        //2.4 开窗窗口
        Table overWindowResultTable = tableEnv.sqlQuery(" select user_name, " +
                " avg(ts) over ( " +
                " partition by user_name " +
                " order by et " +
                //包括当前行和前面的行总共4行的平均时间
                " rows between 3 preceding and current row ) as avg_ts" +
                " from clickTable");


//        tableEnv.toChangelogStream(aggTable).print("agg: ");
//        tableEnv.toChangelogStream(groupWindowResultTable).print("group window: ");
//        tableEnv.toChangelogStream(tumbleWindowResultTable).print("tumble window: ");
//        tableEnv.toChangelogStream(hopWindowResultTable).print("hop window: ");
//        tableEnv.toChangelogStream(cumulateWindowResultTable).print("cumulate window: ");
        tableEnv.toChangelogStream(overWindowResultTable).print("over window: ");

        env.execute();
    }
}
