package Chapter02_Customer_Source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class FilterMap_Function implements FlatMapFunction<Event,String> {


    @Override
    public void flatMap(Event event, Collector<String> collector) throws Exception {
        collector.collect(event.user);
        collector.collect(event.url);
        collector.collect(event.timestamp.toString());

    }
}
