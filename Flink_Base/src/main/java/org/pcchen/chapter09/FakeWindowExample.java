package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 用map状态，模拟滚动窗口
 * 周期统计url的count
 *
 * @author: ceek
 * @create: 2023/3/18 17:10
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        stream.print();

        stream.keyBy(event -> event.getName())
                        .process(new FakeWindowResult(10000l))
                        .print();

        env.execute();
    }

    //实现自定义的keyedProcessFunction，用mapstate进行滚动窗口统计每个url的count值
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String>{
        //定义一个窗口大小
        private Long windowSize;
        //定义存储每个时间窗口及count值mapstate
        //<窗口起始时间戳，窗口中对应key分组后url的对应count>
        private MapState<Long, Long> windowUrlAndCountMapState;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlAndCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("map-state", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //首先定义窗口的其实和结束时间
            Long windowStart = event.getTimestamp() / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            //将统计到的数据更新到mapstate中
            if(windowUrlAndCountMapState.contains(windowStart)) {
                windowUrlAndCountMapState.put(windowStart, windowUrlAndCountMapState.get(windowStart) + 1);
            } else {
                windowUrlAndCountMapState.put(windowStart, 1l);
                //并注册一个windowsize秒后的定时器
                ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            long pv = windowUrlAndCountMapState.get(windowStart);
            String url = ctx.getCurrentKey();

            out.collect( "url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));

            //清空定时器
            windowUrlAndCountMapState.remove(windowStart);
        }
    }
}
