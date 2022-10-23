package com.yumchina.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount_1 {
    public static void main(String[] args) throws Exception {

        //1, 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2, 监听数据接口
        DataStreamSource<String> StreamDataSource = env.socketTextStream("localhost", 7777);
        //3, 数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> WordAndOne = StreamDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        //4, 按照Word分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = WordAndOne.keyBy(data -> data.f0);
        //5, 按照Word进行求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        //6, 打印结果
        sum.print();
        //7, 持续执行环境
        env.execute();
    }
}
