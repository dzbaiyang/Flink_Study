package Chapter02_Customer_Source;


//通过添加AddSource调用一个自定义的Source作为源

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //自定义SourceFunction
//        DataStreamSource<Event> customersource = env.addSource(new ClickSource());

        //定义一个并行度为4的 ParallelismSourceFunction
          DataStreamSource<Event> customersource = env.addSource(new Parallelism_Source()).setParallelism(2);

        customersource.print();

        env.execute();
    }
}
