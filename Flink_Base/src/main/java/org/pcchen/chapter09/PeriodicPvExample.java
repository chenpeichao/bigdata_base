package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * 通过值状态保存pv和定时时间进行，周期性计算PV
 * @author: ceek
 * @create: 2023/3/16 8:29
 */
public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.getTimestamp();
                            }
                        }));

        streamOperator.print();

        //统计每个用户周期性的pv值
        // 统计每个用户的pv，隔一段时间（n秒）输出一次结果
        streamOperator.keyBy(event -> event.getName())
                        .process(new PeriodicPvResult(10l))
                        .print();

        env.execute();
    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        private ValueState<Long> pvCount;
        private ValueState<Long> timerTSState;
        private Long limitTimeSecond = 0l;

        public PeriodicPvResult(Long limitTimeSecond) {
            this.limitTimeSecond = limitTimeSecond;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            pvCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countValueState", Long.class));
            timerTSState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTSState", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 更新count值
            Long count = pvCount.value();
            if (count == null){
                pvCount.update(1L);
            } else {
                pvCount.update(count + 1);
            }
            pvCount.update(pvCount.value() == null ? 1l : pvCount.value()+1);
            //注册定时器，进行定时读取pv数
            if(timerTSState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + limitTimeSecond *1000l);
                timerTSState.update(value.getTimestamp() + limitTimeSecond *1000l);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "的pvCount在" + new Timestamp(timerTSState.value()) + "时的值为" + pvCount.value());
            timerTSState.clear();
            pvCount.clear();
        }
    }
}
