package org.pcchen.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合算子示例
 *
 * @author: ceek
 * @create: 2023/1/4 10:11
 */
public class TransSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./prod?id=4", 5000L),
                new Event("Mary", "./prod?id=3", 3000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./info", 4000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        //max只更新max字段，其余字段不更新
        //maxby得到指定字段的数据，其余字段也是取指定字段对应的数据
//        stream.keyBy(event -> event.getName()).max("timestamp").print("max");
//        stream.keyBy(event -> event.getName()).maxBy("timestamp").print("maxBy:");
        stream.keyBy(event -> event.getName()).sum("timestamp").print("sum");

        env.execute();
    }
}
