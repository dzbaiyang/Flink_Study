package main.java.Chapter02_Customer_Source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransForReduce {
    public static void main(String[] args) throws Exception{

        //1, 创建运行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2, 添加一个方法，自动生成数据；
        DataStreamSource<Event> stream = env.addSource(new Parallelism_Source());

        //1, 统计用户的访问频次

        SingleOutputStreamOperator<Tuple2<String, Long>> ClicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> event1, Tuple2<String, Long> event2) throws Exception {
        //通过上一次的统计结果+下一次的统计结果，通过user来进行keyby，类似与sum group by的概念
                        return Tuple2.of(event1.f0, event1.f1 + event2.f1);
                    }
                });

        //2, 选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = ClicksByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> event1, Tuple2<String, Long> event2) throws Exception {
                        return event1.f1 > event2.f1 ? event1 : event2;
                    }
                });

        result.print();
        env.execute();
    }
}
