package org.pcchen.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单下单到支付cep处理，以及超时事件的处理
 *
 * @author: ceek
 * @create: 2023/3/28 11:34
 */
public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取订单事件流，并提取时间戳、生成水位线
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
//                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 11 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent event, long l) {
                                                return event.getTimestamp();
                                            }
                                        }
                                )
                );

        //定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getEventType().equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getEventType().equals("pay");
                    }
                })
                .within(Time.minutes(15));

        //3、将模式应用到订单数据流上
//        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(orderEvent -> orderEvent.getUserId()), pattern);
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(new KeySelector<OrderEvent, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(OrderEvent value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getOrderId());
            }
        }), pattern);

        //4、定义一个侧输出流
        OutputTag<OrderEvent> outputTag = new OutputTag<OrderEvent>("sideOutPut"){};
        //5、将完全匹配和超时部分匹配的复杂事件提取出来，进行处理
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayPatternProcessFunction());

        //6、打印输出
        result.print("payOrder: ");
        result.getSideOutput(outputTag).print("output: ");

        env.execute();
    }

    //自定义PatternProcessFunction
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {

        //匹配到pay的信息
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            //获取当前的支付事件
            OrderEvent createEvent = match.get("create").get(0);
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("订单 " + payEvent.getOrderId() + "创建为 " + createEvent.getEventType() + " 已支付！");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            // 处理超时未支付事件，输出到侧输出流
            OutputTag<OrderEvent> outputTag = new OutputTag<OrderEvent>("sideOutPut"){};
            ctx.output(outputTag, match.get("create").get(0));
        }
    }
}
