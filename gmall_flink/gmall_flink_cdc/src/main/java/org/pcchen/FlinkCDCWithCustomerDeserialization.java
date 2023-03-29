package org.pcchen;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinkCDC通过自定义反序列化器进行数据读取
 *
 * @author: ceek
 * @create: 2023/3/29 20:41
 */
public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("root_123")
                .databaseList("gmall_flink")
                .tableList("gmall_flink.base_trademark")        //如果不添加该参数,则消费指定数据库中所有表的数据.如果指定,指定方式为db.table
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(mysqlSource);

        //3.打印数据
        dataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}
