package main.java.Chapter02_Customer_Source;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransforFilter {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0, 我这里懒的去写了，直接添加了一个SourceFunction来生成数据；
        DataStreamSource<Event> stream = env.addSource(new Parallelism_Source()).setParallelism(2);

        //1. 可以是实现了FilterFunction的类的对象；
        SingleOutputStreamOperator<Event> filter1 = stream.filter(new Filter_Function());

        //2,传入一个Lambda FilterFunction的接口
        SingleOutputStreamOperator<Event> filter2 = stream.filter(data -> data.user.equals("Ling"));

        filter1.print();
        filter2.print("Lambda: Ling");
        env.execute();
    }

}
