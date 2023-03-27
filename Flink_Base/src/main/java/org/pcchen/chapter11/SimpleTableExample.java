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
 * tableApi和sql的简单示例，进行table和datastream的相互转换
 *
 * @author: ceek
 * @create: 2023/3/19 14:41
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.stream流读取数据
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        //2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将dataStream转换成table
        Table table = tableEnv.fromDataStream(eventStream);

        //4.进行数据操作
        //4.1、直接写sql进行查询
        Table resultTable1 = tableEnv.sqlQuery("select `name`, `urls` from " + table);
        //4.2、基于table的api进行转换
        Table resultTable2 = table.select($("name"), $("urls"))
                .where($("name").isEqual("Alice"));

        //5.转换成数据流打印输出
//        tableEnv.toDataStream(resultTable1).print("result1:");
//        tableEnv.toDataStream(resultTable2).print("result2:");

        tableEnv.createTemporaryView("clickTable", resultTable1);
        //6.对于聚合数据的控制台打印；转换成流
        Table table1 = tableEnv.sqlQuery("select `name`, count(`name`) from clickTable group by `name`");
        tableEnv.toChangelogStream(table1).print("agg");
        env.execute();
    }
}
