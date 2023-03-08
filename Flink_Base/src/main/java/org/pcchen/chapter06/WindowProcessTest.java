package org.pcchen.chapter06;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * process全窗口聚合
 *
 * @author: ceek
 * @create: 2023/3/6 22:24
 */
public class WindowProcessTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> stream = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)    //由于是顺序读取数据，所以不用添加延迟时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );
        //打印窗口里的数据
        stream.print();

        stream
                .keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                //将窗口数据放在一起；看起来效率更低，但是可以拿到更多数据比如：窗口的时间相关信息
                .process(new UVCountByWindow())
                .print();

        env.execute();
    }

    //实现自定义ProcessWindowFunction，输出一条统计信息
    public static class UVCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userHashSet = new HashSet<String>();
            for(Event event : elements) {
                userHashSet.add(event.getName());
            }
            Integer uv = userHashSet.size();    //用户uv

            Long start = context.window().getStart();   //窗口起始时间
            Long end = context.window().getEnd();       //窗口截止时间

            out.collect("窗口: " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " 的独立访客数量是：" + uv);
        }
    }
}
