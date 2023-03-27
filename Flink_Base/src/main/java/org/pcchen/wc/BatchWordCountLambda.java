package org.pcchen.wc;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Lambda表达式实现批处理wordcount
 *
 * @author: ceek
 * @create: 2022/9/15 15:20
 */
public class BatchWordCountLambda {
    public static void main(String[] args) throws Exception {
        //1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2、从文件中读取数据，按行读取(存储的数据就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("Flink_Base\\input\\word.txt");

        //3、数据格式转换(扁平化处理)
        //  参数一：输入的数据类型
        //  参数二：转换后输出的数据(匿名函数内部，会使用Collector将数据收集起来)，使用二元组进行
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                // 当lambda表达式使用泛型时，会进行泛型擦除，需要显示的声明类型信息
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4、按照word进行分组，0表示分组索引；表示二元组中的第一个数据
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOneTuple.groupBy(0);

        //5、sum聚合函数，参数1为索引，指以二元组第二个数据进行求和
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        //6、数据打印
        sum.print();
    }
}
