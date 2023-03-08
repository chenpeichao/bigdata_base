package org.pcchen.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 处理函数-包含事件时间的定时器(需基于keyedstream的处理函数)
 *
 * @author: ceek
 * @create: 2023/3/7 16:53
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(1909);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.keyBy(event -> event.getName())
                .process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                //创建一个定时器进行数据操作
                Long currTs = ctx.timestamp();
                out.collect(ctx.getCurrentKey() + "的数据到达，事件时间为：" + new Timestamp(currTs) + "，水位线为：" +new Timestamp(ctx.timerService().currentWatermark()));

                //注册一个5秒后执行的定时器---触发是根据水位线时间，所以当时间时间为110001时，水位线为11000时，触发了定时器
                ctx.timerService().registerEventTimeTimer(currTs + 10*1000);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect( ctx.getCurrentKey()+ "的定时器执行，时间为：" + new Timestamp(timestamp)+ "，水位线为：" +new Timestamp(ctx.timerService().currentWatermark()));
            }
        }).print();

        env.execute();
    }

    //自定义测试数据数据源
    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            //直接发出数据
            ctx.collect(new Event("Alice", "./prod?id=1", 1000L));
            Thread.sleep(5000);
            ctx.collect(new Event("Bob", "./prod?id=1", 11000L));
            Thread.sleep(5000);
            ctx.collect(new Event("Mary", "./prod?id=1", 11001L));
            Thread.sleep(20000);
        }

        @Override
        public void cancel() {

        }
    }
}
