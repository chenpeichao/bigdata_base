package org.pcchen.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义udf聚合函数即：多对一
 *
 * @author: ceek
 * @create: 2023/3/27 19:49
 */
public class UdfTest_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1、在创建表的DDL语句中直接定义时间语义
        String createDDL = " CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                " ) WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'Flink_Base\\input\\clicks.txt', " +
                " 'format' = 'csv' " +
                " )";
        tableEnv.executeSql(createDDL);

        //2、注册自定义表函数
        tableEnv.createTemporaryFunction("WeightedAverage", WeightedAverage.class);
        //3、调用udf进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user_name, " +
                " WeightedAverage(ts, 1) as weightedAvg" +
                " from clickTable " +
                " group by user_name");
        //4、转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print("agg func: ");

        env.execute();
    }

    // 单独定义一个累加器类型
    public static class WeightedAvgAccumulator {
        public long sum = 0;    // 加权和
        public int count = 0;    // 数据个数
    }

    //实现自定义聚合函数，进行加权平均数计算
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0)
                return null;    // 防止除数为0
            else
                return accumulator.sum / accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加计算方法，类似于add
        //第一个参数：accumulate聚合结果类型
        //第二个参数(即其它参数)：函数传进来的参数
        //第三个参数(即其它参数)：函数传进来的参数
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight){
            accumulator.sum += iValue * iWeight;    // 这个值要算iWeight次
            accumulator.count += iWeight;
        }
    }
}
