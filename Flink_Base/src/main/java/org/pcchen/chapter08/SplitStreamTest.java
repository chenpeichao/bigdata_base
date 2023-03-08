package org.pcchen.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 分流测试
 *
 * @author: ceek
 * @create: 2023/3/8 15:14
 */
public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        //定义outputtag进行分流
        OutputTag<Event> marryTag = new OutputTag<Event>("marryTag"){};
        OutputTag<Event> bobTag = new OutputTag<Event>("bobTag"){};
        SingleOutputStreamOperator<String> resultOpt = streamOperator.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if (event.getName().equals("Marry")) {
                    ctx.output(marryTag, event);
                } else if (event.getName().equals("Bob")) {
                    ctx.output(bobTag, event);
                }
                out.collect(event.toString());
            }
        });
        resultOpt.print("all->");
        resultOpt.getSideOutput(marryTag).print("marryTag->");
        resultOpt.getSideOutput(bobTag).print("bobTag->");


        env.execute();
    }
}
