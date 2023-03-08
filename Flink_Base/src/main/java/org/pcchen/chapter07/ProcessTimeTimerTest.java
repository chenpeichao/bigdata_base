package org.pcchen.chapter07;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;

/**
 * 处理函数-包含处理时间的定时器(需基于keyedstream的处理函数)
 *
 * @author: ceek
 * @create: 2023/3/7 16:32
 */
public class ProcessTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(1909);

        // 由于是处理时间，所以不用加水位线，没什么用
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        OutputTag<String> outputTag = new OutputTag<String>("1"){};
        // 要用定时器，必须基于KeyedStream
        SingleOutputStreamOperator<String> process = stream.keyBy(event -> event.getName())
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        //创建一个定时器进行数据操作
                        Long currTs = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + "的数据到达，处理时间为：" + new Timestamp(currTs));

                        //注册一个5秒后执行的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 5 * 1000);

                        //将数据放到侧输出流中，进行处理
                        ctx.output(outputTag, value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "的定时器执行，时间为：" + new Timestamp(timestamp));
                    }
                });
        process.print("process==>");

        process.getSideOutput(outputTag).print("late data===>");

        env.execute();
    }
}
