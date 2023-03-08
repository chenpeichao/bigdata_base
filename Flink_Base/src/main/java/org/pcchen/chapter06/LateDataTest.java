package org.pcchen.chapter06;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 延迟数据获取及处理
 * 一：通过水位线处理
 * 二：通过sideOutputLateData迟到数据处理
 * 三：已经超过迟到延时时间到达的数据，放到侧输出流中进行处理
 *
 * @author: ceek
 * @create: 2023/3/7 9:17
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> input = env.socketTextStream("192.168.1.101", 7777)
                .map(line -> {
                    String[] words = line.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
                })
                //方式一：设置watermark延迟时间为2s
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );
        input.print("input");

        //定义一个侧输出流的标签(输出的是数据的原始类型)---会泛型擦除，所以使用匿名类定义
        OutputTag<Event> eventOutputTag = new OutputTag<Event>("late"){};

        //统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = input.keyBy(event -> event.getUrls())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //延迟触发窗口，也是根据水位线触发即；1分钟+窗口时间10s+水位线时间2秒
                //方式二：允许窗口处理迟到数据，设置一分钟的等到时间
                .allowedLateness(Time.minutes(1))
                //方式三：将最后迟到的数据放到侧输出流中
                .sideOutputLateData(eventOutputTag)
                .aggregate(new UrlCountViewAgg(), new UrlCountViewProcess());
        result.print();

        //获取侧输出流
        result.getSideOutput(eventOutputTag).print("late");

        env.execute();
    }

    //对于数据聚合
    public static class UrlCountViewAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
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

    //输出信息
    public static class UrlCountViewProcess extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(s, elements.iterator().next(), start, end));
        }
    }
}
