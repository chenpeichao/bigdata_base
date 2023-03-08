package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;
import java.util.Calendar;

/**
 * 窗口使用reduce
 *
 * @author: ceek
 * @create: 2023/3/6 11:54
 */
public class WindowReduceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> sourceStream = env.addSource(new ClickSource());
        sourceStream
                //定义水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.getTimestamp();
                            }
                        }))
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.getName(), 1L);
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((tuple1, tuple2) ->
                    Tuple2.of(tuple1.f0, tuple1.f1 + tuple1.f1)
                )
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                    }
//                })
                .print(Calendar.getInstance().getTimeInMillis() + "");

        env.execute();
    }
}
