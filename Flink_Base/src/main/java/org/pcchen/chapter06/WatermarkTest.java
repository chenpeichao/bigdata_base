package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 水位线测试示例
 * 水位线的生成：需要水位线的生成器(WatermarkGenerator)以及时间戳的提取器(TimestampAssigner)
 *
 * @author: ceek
 * @create: 2023/1/5 17:16
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

        env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Alice", "./prod?id=200", 3500L),
                        new Event("Bob", "./prod?id=2", 2500L),
                        new Event("Alice", "./prod?id=300", 3600L),
                        new Event("Bob", "./home", 3000L),
                        new Event("Bob", "./prod?id=1", 2300L),
                        new Event("Bob", "./prod?id=3", 3300L))
                //有序流的watermark生成
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                                    @Override
//                                    public long extractTimestamp(Event element, long recordTimestamp) {
//                                        return element.getTimestamp();
//                                    }
//                                })
//                )
                //乱序流的水位线
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        // 抽取时间戳的逻辑
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.getTimestamp();
                                            }
                                        }
                                )
                );

        env.execute();

    }
}
