package org.pcchen.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * 使用aggregate和process全窗口函数集合统计uv
 *
 * @author: ceek
 * @create: 2023/3/6 22:52
 */
public class UVCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> streamOperator = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );
        streamOperator.print();

        //使用aggregateFunction和processFUnction全窗口函数结合计算uv
        streamOperator.keyBy(event -> true)   //对于全数据计算pv / uv所以不用分组
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UVAgg(), new UVCountResult())
                .print();


        env.execute();
    }

    //自定义实现aggregateFunction，增量计算uv
    public static class UVAgg implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> accumulator) {
            accumulator.add(event.getName());
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    //自定义实现processFUnction，实现窗口信息输出
    //上面agg自定义函数的getResult的输出就是此处的输入，即为一个Long的值uv
    public static class UVCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        //结合类上注释：此处的elements中有且只有一个值：uv
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long uv = elements.iterator().next();

            Long start = context.window().getStart();
            Long end = context.window().getEnd();

            out.collect("窗口: " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " 的独立访客数量是：" + uv);
        }
    }
}
