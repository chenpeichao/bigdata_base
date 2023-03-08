package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 时间窗口测试
 *
 * @author: ceek
 * @create: 2023/3/6 11:32
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

        SingleOutputStreamOperator<Event> streamOperator = env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Alice", "./prod?id=200", 3500L),
                        new Event("Bob", "./prod?id=2", 2500L),
                        new Event("Alice", "./prod?id=300", 3600L),
                        new Event("Bob", "./home", 3000L),
                        new Event("Bob", "./prod?id=1", 2300L),
                        new Event("Bob", "./prod?id=3", 3300L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                }
                        ));

        streamOperator.keyBy(event -> event.getName())
                .window(TumblingEventTimeWindows.of(Time.hours(1)));    //滚动事件时间窗口
//                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))    //滚动处理时间窗口(下同省略)
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))    //滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))    //事件时间会话窗口
//                .countWindow(10, 2)    //滑动计数窗口

        env.execute();
    }
}
