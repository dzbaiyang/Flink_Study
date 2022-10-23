package main.java.yumchina.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args)  throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        // 3. 将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    // 4. 将一行文本进行分词
                    String[] words = line.split(" ");
                    // 5. 将每个单词转换为二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 6. 按照Word分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        // 7. 分组内进行聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

//        8. 打印结果
        sum.print();

    }
}
