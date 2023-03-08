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

import java.time.Duration;

/**
 * url的访问数在窗口中并返回包装类通过aggregate和process组合实践
 *
 * @author: ceek
 * @create: 2023/3/6 23:16
 */
public class URLCountViewExample {
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
        streamOperator.keyBy(event -> event.getUrls())   //对于全数据计算pv / uv所以不用分组
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new URLCountViewAgg(), new URLCountViewResult())
                .print();


        env.execute();
    }

    //增量聚合，来一条数据就+1
    public static class URLCountViewAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class URLCountViewResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            UrlViewCount urlViewCount = new UrlViewCount();
            urlViewCount.setCount(elements.iterator().next());
            urlViewCount.setUrl(s);
            urlViewCount.setWindowStart(context.window().getStart());
            urlViewCount.setWindowEnd(context.window().getEnd());
            out.collect(urlViewCount);
        }
    }
}
