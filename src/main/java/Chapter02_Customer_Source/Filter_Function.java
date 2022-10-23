package main.java.Chapter02_Customer_Source;


import org.apache.flink.api.common.functions.FilterFunction;

public class Filter_Function implements FilterFunction<Event> {

    @Override
    public boolean filter(Event event) throws Exception {
        return event.user.equals("Bob");
    }
}