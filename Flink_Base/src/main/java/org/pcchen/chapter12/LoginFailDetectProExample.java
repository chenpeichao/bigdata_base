package org.pcchen.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 通过cep进行连续登录失败精简版实现
 *
 * @author: ceek
 * @create: 2023/3/28 11:26
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 1000l),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        loginEventStream.print("input: ");

        //2、定义模式，连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")    //第一次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                }).times(3).consecutive();  //指定为严格紧邻的三次登录失败

        //3、将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(loginEvent -> loginEvent.getUserId()), pattern);
        //4、将检查到的复杂事件提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                LoginEvent first = pattern.get("fail").get(0);
                LoginEvent second = pattern.get("fail").get(0);
                LoginEvent third = pattern.get("fail").get(0);
                return first.getUserId() + " 连续三次登录失败！登录时间：" + new Timestamp(first.getTimestamp()) + ", " + new Timestamp(second.getTimestamp()) + ", " + new Timestamp(third.getTimestamp());
            }
        });

        //打印输出
        warningStream.print();

        env.execute();
    }
}
