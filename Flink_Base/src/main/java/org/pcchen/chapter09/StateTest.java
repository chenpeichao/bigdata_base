package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;

/**
 * 常见state测试
 *
 * @author: ceek
 * @create: 2023/3/9 8:22
 */
public class StateTest {
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

        stream.print("input: ");

        stream.keyBy(event -> event.getName())
                .flatMap(new MyFlatMap())
                .print("resutl: ");

        env.execute();
    }

    // 实现自定义的FlatMapFunction，用于Keyed State测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        private ValueState<Event> myValueState;
        private ListState<Event> myListState;
        private MapState<String, Long> myMapState;
        private ReducingState<Event> myReducingState;
        private AggregatingState<Event, String> myAggState;
        // 增加一个本地变量进行对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            //给value状态设置ttl，通过descriptor设置--本次在方法末尾设置
            ValueStateDescriptor myValueStateDesriptor = new ValueStateDescriptor("my-value", Event.class);

            myValueState = getRuntimeContext().getState(myValueStateDesriptor);
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event(value1.getName(), value1.getUrls(), value2.getTimestamp());
                }
            }, Event.class));

            myAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-aggregate", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0l;
                }

                @Override
                public Long add(Event value, Long accumulator) {
                    return accumulator + 1l;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "accCount = " + accumulator.intValue();
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, Long.class));

            //对于状态的ttl设置
            StateTtlConfig.newBuilder(Time.hours(1))
                    //设置更新类型
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    //设置状态的可见性--表示过期还存在，则返回
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    //设置状态的可见性--表示过期不能返回
//                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //value访问和更新状态
            if(value.getName().trim().equals("Bob")) {
                System.out.println("value的上一次值：" + myValueState.value());
                myValueState.update(value);
                System.out.println("value更新后的值" + myValueState.value());
            }
            //list的更新和使用
            if(value.getName().trim().equals("Alice")) {
                myListState.add(value);
            }
            //map的更新和使用
            myMapState.put(value.getName(), myMapState.isEmpty() ? 1l : myMapState.get(value.getName()) + 1l);
            for(String key : myMapState.keys()) {
                out.collect(key + "->" + myMapState.get(key));
            }

            //reduce的更新和使用
            myReducingState.add(value);
            System.out.println("reduce latest value ::" + myReducingState.get());
            //agg的更新和使用
            System.out.println(value);
            myAggState.add(value);
            System.out.println(value.getName() + "最新的统计count为：" + myAggState.get());

            //此变量不受key的划分，统计所有数据的count值
            count++;
            System.out.println("temp coumt:" + count);
        }
    }
}
