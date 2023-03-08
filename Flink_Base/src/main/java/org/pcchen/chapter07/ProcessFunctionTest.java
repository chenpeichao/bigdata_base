package org.pcchen.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 处理函数
 *
 * @author: ceek
 * @create: 2023/3/7 15:49
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(1909);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.process(new ProcessFunction<Event, Object>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Object>.Context ctx, Collector<Object> out) throws Exception {
                if(value.getName().equals("Bob")) {
                    out.collect(value.getName() + " clicks " + value.getUrls());
                } else if(value.getName().equals("Alice")) {
                    out.collect(value.getName());
                    out.collect(value.getName());
                }

                System.out.println("timestamp:" + new Timestamp(ctx.timestamp()));
                System.out.println("watermark:" + (ctx.timerService().currentWatermark() > 0 ? new Timestamp(ctx.timerService().currentWatermark()) :ctx.timerService().currentWatermark()));

                System.out.println("indexOfThisSubtask:" + getRuntimeContext().getIndexOfThisSubtask());

                out.collect("-----------------\n");
            }
        }).print();

        env.execute();
    }
}
