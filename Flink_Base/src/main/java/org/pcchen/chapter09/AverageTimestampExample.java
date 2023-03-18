package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 通过aggstate实现每隔5个数据统计平均时间戳
 *
 * @author: ceek
 * @create: 2023/3/18 17:53
 */
public class AverageTimestampExample {
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

        stream.print("input:");

        stream.keyBy(event -> event.getName())
                .flatMap(new AvgTsResult(5l))
                .print("resutl: ");

        //countWindow也可以实现
        /*stream.keyBy(event -> event.getName())
                .countWindow(3, 3)    //滑动计数窗口
                .process(new ProcessWindowFunction<Event, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Event, String, String, GlobalWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        Long timeSum = 0l;
                        Iterator<Event> iterator = elements.iterator();
                        while(iterator.hasNext()) {
                            timeSum += iterator.next().getTimestamp();
                        }
                        out.collect(s + " 过去：" + 10 + "次访问平均时间戳为：" + new Timestamp(timeSum / 3));
                    }
                })
                .print("result: ");*/

        env.execute();
    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        //定义每隔多少个数据统计的指标数据
        private Long countSize;
        //定义一个值状态，用来表示当前key下共多少个数据
        private ValueState<Long> countState;
        //定义一个聚合状态，用来保存平均时间戳
        private AggregatingState<Event, Long> avgAggState;

        public AvgTsResult(Long countSize) {
            this.countSize = countSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            avgAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "agg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0l, 0l);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            accumulator.setField(accumulator.f0 + value.getTimestamp(), 0);
                            accumulator.setField(accumulator.f1 + 1, 1);
                            return accumulator;
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            Long currentCount = countState.value();

            if(currentCount == null) {
                countState.update(1l);
            } else {
                countState.update(currentCount + 1);
            }

            avgAggState.add(value);
            if(countState.value().equals(countSize)) {
                out.collect(value.getName() + " 过去：" + countSize + "次访问平均时间戳为：" + new Timestamp(avgAggState.get()));

                countState.clear();
                avgAggState.clear();
            }
        }
    }
}
