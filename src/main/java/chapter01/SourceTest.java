package main.java.chapter01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception{

        // 1, 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2, 从文件直接读取
        DataStreamSource<String> Stream1 = env.readTextFile("D:\\01_Code\\YumchinaFlink\\input\\words.txt");

        //3, 代码当中嵌入包装成一个集合类型
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(3);
        DataStreamSource<Integer> Stream2 = env.fromCollection(nums);

        //--------------------------------------------------------------
        ArrayList<envent> envents = new ArrayList<>();
        envents.add(new envent("Bob","/home",4300L));
        envents.add(new envent("Marry","/user",9999L));
        DataStreamSource<envent> Stream3 = env.fromCollection(envents);

        //3, 从元素读取数据

        DataStreamSource<envent> Stream4 = env.fromElements(
                new envent("Ling", "/home", 4300L),
                new envent("Jessies", "/user", 9999L)
        );

        //4, 从Socket读取数据
//        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 7777);


        //5, 从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","0.0.0.0:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

//        Stream1.print();
//        Stream2.print("nums");
//        Stream3.print("Envents");
//        Stream4.print("Elements");
        kafkaStream.print();
//        socketSource.print("Stream");
        env.execute();

    }
}
