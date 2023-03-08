package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 窗口聚合aggregate
 * 获取到每个用户的平均访问时间
 *
 * @author: ceek
 * @create: 2023/3/6 17:37
 */
public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)    //由于是顺序读取数据，所以不用添加延迟时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        )
            .keyBy(event -> event.getName())
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                @Override
                public Tuple2<Long, Integer> createAccumulator() {
                    //创建累加器
                    return Tuple2.of(0l, 0);
                }

                @Override
                public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                    //属于本窗口的数据来一条累加一次，并返回累加器
                    return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
                }

                @Override
                public String getResult(Tuple2<Long, Integer> accumulator) {
                    //窗口闭合时，增量聚合结束，将计算结果发送到下游
                    Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                    return timestamp.toString();
                }

                @Override
                public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                    return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                }
            })
            .print();

        env.execute();
    }
}
