package org.pcchen.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 常用的table流获取(新blink及老版本中得到tableEnv)以及表的基本操作
 * @author: ceek
 * @create: 2023/3/20 10:52
 */
public class CommonApiTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
//        //得到table流
//        env.setParallelism(1);
//        env.execute();

        //1.定义环境配置来创建表执行环境
        //  1.1 基于blink版本planner进行流处理
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableStreamBlinkEnv = TableEnvironment.create(blinkSettings);

        //  1.2 基于老版本planner进行流处理
//        EnvironmentSettings oldSettings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();
//        TableEnvironment tableStreamOldEnv = TableEnvironment.create(oldSettings);
//
//        //  1.3 基于blink版本planner进行批处理
//        EnvironmentSettings blinkBatchEvnSettings = EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//        TableEnvironment tableBatchBlinkEnv = TableEnvironment.create(blinkBatchEvnSettings);
//
//        //  1.4 基于老版本进行批处理
//        ExecutionEnvironment oldBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableBatchOldEnv = BatchTableEnvironment.create(oldBatchEnv);

        //2 创建表
        String createInputTableSQL = "CREATE TABLE clickTable ( " +
                " `username` STRING, " +
                " urls STRING, " +
                " `timestamp` BIGINT " +
                " ) WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'Flink_Base\\input\\clicks.txt', " +
                " 'format' = 'csv' " +
                " ) ";
        String createDDLOutputSQL = "CREATE TABLE outTable (" +
                " user_name STRING, " +
//                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'Flink_Base\\output', " +
                " 'format' =  'csv' " +
                ")";
        //创建控制台打印的输出表
        String createPrintOutSQL = " CREATE TABLE printOutTable ( " +
                "username String, " +
                "`timestamp` BIGINT " +
                ") with ( " +
                "'connector' = 'print' " +
                ")";
        tableStreamBlinkEnv.executeSql(createInputTableSQL);

        //创建控制台打印聚合表
        String createAGGPrintOutSQL = "CREATE TABLE aggprintOutTable ( " +
                "username STRING, " +
                "`count` BIGINT " +     //由下面插入的字段可知，数据类型要一样，名称可不一样
                ") WITH ( " +
                "'connector' = 'print' " +
                ")";
        tableStreamBlinkEnv.executeSql(createAGGPrintOutSQL);

        //下面介绍两种方式定义查询
        //env.sqlQuery：表示执行query语句
        //env.executeSql：表示执行表的ddl语句
        //env.from：表示从表明获取talbe

        Table initClickTable = tableStreamBlinkEnv.from("clickTable");
        //2.1 数据查询使用api的方式
        Table twoColTmpTable = initClickTable.where($("username").isEqual("Alice"))
                .select($("username"), $("timestamp"));
        //2.11 创建临时表
        tableStreamBlinkEnv.createTemporaryView("tmpClickTable", twoColTmpTable);
        //2.2 数据查询使用sql字符串的方式
        Table sqlStrCliTable = tableStreamBlinkEnv.sqlQuery("select username, `timestamp` from clickTable ");
        //2.3 聚合查询
        Table aggTable = tableStreamBlinkEnv.sqlQuery(" select username, count(username) as cnt from clickTable group by username ");

        //3 定义输出表
        tableStreamBlinkEnv.executeSql(createDDLOutputSQL); //文件输出
        tableStreamBlinkEnv.executeSql(createPrintOutSQL); //控制台打印

        //4 将查询出来的数据放到输出表中
//        sqlStrCliTable.executeInsert("outTable");
//        sqlStrCliTable.executeInsert("printOutTable");
        aggTable.executeInsert("aggprintOutTable");
    }
}
