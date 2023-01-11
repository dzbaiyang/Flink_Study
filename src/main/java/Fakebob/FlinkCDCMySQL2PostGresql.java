package main.java.Fakebob;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDCMySQL2PostGresql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
        configuration.setString("table.exec.sink.not-null-enforcer", "drop");
        env.setParallelism(1);
        //获取MySQL数据
        DebeziumSourceFunction<String> MySQLFuncation = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("hirisun")
                .databaseList("classicmodels")
                .tableList("classicmodels.customers")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .serverTimeZone("GMT")
                .build();
        DataStreamSource<String> dataStreamSource  = env.addSource(MySQLFuncation);
        dataStreamSource.print();
        env.execute();

    }
}
