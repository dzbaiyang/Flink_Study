package main.java.yumchina.mysql;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class getmysql_cdc_pgsql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取MySQL数据
        DebeziumSourceFunction<String> MySQLFuncation = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("hirisun")
                .databaseList("classicmodels")
                .tableList("classicmodels.orderdetails")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .serverTimeZone("GMT")
                .build();
        DataStreamSource<String> dataStreamSource  = env.addSource(MySQLFuncation);
        dataStreamSource.print();
        env.execute();
    }
}