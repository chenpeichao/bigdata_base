package org.pcchen.chapter09;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * 广播流应用对于用户行为模式关联关系统计
 * 待完善，当action和pattern顺序不一致时，读取不到数据
 *
 * @author: ceek
 * @create: 2023/3/18 20:09
 */
public class BehaviorPatternDetectorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //得到用户行为流
        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );
//        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
//                new Action("Bob", "login"),
//                new Action("Bob", "buy"),
//                new Action("Alice", "login"),
//                new Action("Alice", "pay")
//        );
        //获取模式匹配的流
        DataStreamSource<Pattern> patternDataStreamSource = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );
        //将模式匹配的流进行广播
        BroadcastStream<Pattern> bcPatterns = patternDataStreamSource.broadcast(new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID, Types.POJO(Pattern.class)));
//        // 定义广播状态的描述器，创建广播流
//        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>(
//                "patterns", Types.VOID, Types.POJO(Pattern.class));
//        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        //合并两个流
        actionDataStreamSource.keyBy(action -> action.userId)
                                .connect(bcPatterns)
                                .process(new PatternEvaluator())
                                .print();
        env.execute();
    }
    public static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration conf) {
            prevActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING));
        }

        @Override
        public void processBroadcastElement(
                Pattern pattern,
                Context ctx,
                Collector<Tuple2<String, Pattern>> out) throws Exception {

            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));

            // 将广播状态更新为当前的pattern
            bcState.put(null, pattern);
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx,
                                   Collector<Tuple2<String, Pattern>> out) throws Exception {
            Pattern pattern = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(action.action);
        }
    }

    // 定义用户行为事件POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式POJO类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
    /*public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        //定义一个值状态，进行用户前一个行为的记录
        private ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("pre-action-state", Types.STRING));
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            //从广播流中获取模式
            MapStateDescriptor<Void, Pattern> stateDescriptor = new MapStateDescriptor<>("broadcast-pattern", Types.VOID, Types.POJO(Pattern.class));
            ReadOnlyBroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(stateDescriptor);
            Pattern pattern = broadcastState.get(null);

            if(prevActionState.value() != null && pattern != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevActionState.value()) && pattern.action2.equals(value.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            prevActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            MapStateDescriptor<Void, Pattern> stateDescriptor = new MapStateDescriptor<>("broadcast-pattern", Types.VOID, Types.POJO(Pattern.class));
            BroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(stateDescriptor);

            broadcastState.put(null, value);
        }
    }

    // 定义用户行为事件POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式POJO类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }*/
}
