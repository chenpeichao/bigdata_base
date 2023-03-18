package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * 通过列表状态进行leftjoin操作
 *
 * @author: ceek
 * @create: 2023/3/18 10:32
 */
public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> leftStream = env.fromElements(
                new Tuple2<String, Long>("a", 1000l),
                new Tuple2<String, Long>("b", 2000l),
                new Tuple2<String, Long>("a", 3000l)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        SingleOutputStreamOperator<Tuple2<String, Long>> rightStream = env.fromElements(
                new Tuple2<String, Long>("a", 4000l),
                new Tuple2<String, Long>("b", 5000l),
                new Tuple2<String, Long>("a", 6000l)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        //通过listState列表状态进行leftjoin合流操作
        leftStream.keyBy(element -> element.f0)
                        .connect(rightStream.keyBy(element -> element.f0))
//                        .process(new LeftJoinKeyedCoProcessFunction())
                        .process(new LeftJoinCoProcessFunction())
                        .print();

        env.execute();
    }

    private static class LeftJoinKeyedCoProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>, String> {
        private ListState<Tuple2<String, Long>> stream1ListState;
        private ListState<Tuple2<String, Long>> stream2ListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            stream1ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.LONG))
            );
            stream2ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.LONG))
            );
//            stream1ListState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
//                    }))
//            );
//            stream2ListState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
//                    }))
//            );
        }

        @Override
        public void processElement1(Tuple2<String, Long> left, KeyedCoProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
//            stream1ListState.add(left);
//            for (Tuple2<String, Long> right : stream2ListState.get()) {
//                out.collect(left + " => " + right);
//            }

            stream1ListState.add(left);
            Iterator<Tuple2<String, Long>> iterator = stream1ListState.get().iterator();
            StringBuilder sb = new StringBuilder();
            sb.append("left : =>");
            while (iterator.hasNext()) {
                sb.append(iterator.next() + "\t");
            }
            out.collect(sb.toString());
        }

        @Override
        public void processElement2(Tuple2<String, Long> right, KeyedCoProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
//            stream2ListState.add(right);
//            for (Tuple2<String, Long> left : stream1ListState.get()) {
//                out.collect(left + " => " + right);
//            }

            stream2ListState.add(right);
            Iterator<Tuple2<String, Long>> iterator = stream2ListState.get().iterator();
            StringBuilder sb = new StringBuilder();
            sb.append("right : =>");
            while (iterator.hasNext()) {
                sb.append(iterator.next() + "\t");
            }
            out.collect(sb.toString());
        }
    }

    private static class LeftJoinCoProcessFunction extends CoProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>, String> {
        private ListState<Tuple2<String, Long>> stream1ListState;
        private ListState<Tuple2<String, Long>> stream2ListState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            stream1ListState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<Tuple2<String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.LONG))
//            );
//            stream2ListState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.LONG))
//            );
            stream1ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Long>>("stream1-list", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                    }))
            );
            stream2ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                    }))
            );
        }

        @Override
        public void processElement1(Tuple2<String, Long> left, CoProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            stream1ListState.add(left);
//            for (Tuple2<String, Long> right : stream2ListState.get()) {
//                out.collect(left + " => " + right);
//            }
            Iterator<Tuple2<String, Long>> iterator = stream1ListState.get().iterator();
            StringBuilder sb = new StringBuilder();
            sb.append("left : =>");
            while (iterator.hasNext()) {
                sb.append(iterator.next());
            }
            out.collect(sb.toString());
//            stream1ListState.add(left);
        }

        @Override
        public void processElement2(Tuple2<String, Long> right, CoProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            stream2ListState.add(right);
//            for (Tuple2<String, Long> left : stream1ListState.get()) {
//                out.collect(left + " => " + right);
//            }

            Iterator<Tuple2<String, Long>> iterator = stream2ListState.get().iterator();
            StringBuilder sb = new StringBuilder();
            sb.append("right : =>");
            while (iterator.hasNext()) {
                sb.append(iterator.next() + "\t");
            }
            out.collect(sb.toString());
//            stream2ListState.add(right);
        }

    }
}
