package main.java.Chapter02_Customer_Source;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransforFilterMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //1， 添加一个AddSource方法，其实就是我的数据源；
        DataStreamSource<Event> filterMap = env.addSource(new Parallelism_Source()).setParallelism(1);

        //2, 实现一个自定义FilterMapFunction，其实就是调用一个通用的方法，方法为FilerMap
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator1 = filterMap.flatMap(new FilterMap_Function());


        //3, 实现一个匿名函数的FilterMapFunction，其实就是不用在用调用别的方法来实现；
        SingleOutputStreamOperator<Object> stringSingleOutputStreamOperator2 = filterMap.flatMap(new FlatMapFunction<Event, Object>() {
            @Override
            public void flatMap(Event event, Collector<Object> collector) throws Exception {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<Object>() {
            @Override
            public TypeInformation<Object> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        //4, 实现一个Lambda FilterMapFunction
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator3 = filterMap.flatMap((Event event, Collector<String> collector) -> {
            if (event.user.equals("Bob"))
                collector.collect(event.user);
            else if (event.user.equals("Ling")) {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }

        }).returns(new TypeHint<String>() {
            @Override
            public TypeInformation<String> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

          stringSingleOutputStreamOperator1.print("Custmoer FilterMapFunction：");
          stringSingleOutputStreamOperator2.print("NonFilterMap：");
          stringSingleOutputStreamOperator3.print("Lambda：");

        env.execute();
    }
}
