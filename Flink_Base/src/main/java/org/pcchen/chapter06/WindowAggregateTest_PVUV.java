package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;
import java.util.HashSet;

/**
 * 窗口聚合获取总的pv和uv
 * 通过aggregate一个函数计算pv 和 uv
 *
 * @author: ceek
 * @create: 2023/3/6 17:58
 */
public class WindowAggregateTest_PVUV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> streamOperator = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );
        streamOperator.print();
        streamOperator.keyBy(event -> true)   //对于全数据计算pv / uv所以不用分组
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                    @Override
                    public Tuple2<Long, HashSet<String>> createAccumulator() {
                        //初始化累加器初始状态
                        return Tuple2.of(0l, new HashSet<String>());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
                        //每来一条数据pv+1；uv往set中添加
                        accumulator.f1.add(event.getName());
                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
                        //从累加器中获取结果并计算pv/uv的结果
                        //窗口闭合时，增量聚合结束，将计算结果发送到下游
                        return (double) accumulator.f0 / accumulator.f1.size();
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
                        //合并；由于累加过程没有merge，所以可以不写
                        return null;
                    }
                })
                .print();


        env.execute();
    }
}
