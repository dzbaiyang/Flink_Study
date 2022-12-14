package main.java.yumchina.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 读取文件
        DataStreamSource<String> lineStreamSource = env.readTextFile("input/words.txt");
        //3, 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> WordAndOneTuple = lineStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));

                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 6. 按照Word分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = WordAndOneTuple.keyBy(data -> data.f0);
        // 7. 分组内进行聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        //8. 打印结果
        sum.print();
        //9. 启动执行,正槽
        env.execute();
    }
}
