package org.pcchen.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 合流测试-可合并多个
 *
 * @author: ceek
 * @create: 2023/3/8 15:25
 */
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("192.168.1.101", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2l))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));
        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("192.168.1.101", 8888)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5l))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        stream1.print("stream1->");
        stream2.print("stream2->");
        //通过union进行合流
        stream1.union(stream2)
                        .process(new ProcessFunction<Event, String>() {
                            @Override
                            public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                                //水位线以联合数据的最低的水位线为标准
                                out.collect(" 水位线为：" + ctx.timerService().currentWatermark());
                            }
                        }).print("all");


        env.execute();
    }
}
