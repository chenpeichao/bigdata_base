package org.pcchen.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * join测试
 *
 * @author: ceek
 * @create: 2023/3/8 16:23
 */
public class JoinedStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("c", 4000L),
                        Tuple2.of("b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        DataStream<Tuple2<Long, String>> stream2 = env
                .fromElements(
                        Tuple2.of(3000L, "a"),
                        Tuple2.of(3000L, "b"),
                        Tuple2.of(4000L, "a"),
                        Tuple2.of(4000L, "b")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<Long, String>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<Long, String> stringLongTuple2, long l) {
                                                return stringLongTuple2.f0;
                                            }
                                        }
                                )
                );

        stream1
                .join(stream2)
                //指定第一条流中的key
                .where(r -> r.f0)
                //指定第二条流中的key
                .equalTo(r -> r.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<Long, String>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> left, Tuple2<Long, String> right) throws Exception {
                        return left + "=>" + right;
                    }
                })
                .print();

        env.execute();
    }
}
