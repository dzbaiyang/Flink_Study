package main.java.yumchina.postgresql;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class csp_postgresql_cdc {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//1, 设置环境变量
        SourceFunction<String> pgsource = PostgreSQLSource.<String>builder()
                .hostname("0.0.0.0")
                .port(5432)
                .database("postgres")
                .schemaList("public") // monitor all tables under inventory schema
                .username("postgres")
                .password("postgres")
                .tableList("cdc_pg_source")
                .decodingPluginName("pgoutput")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        //3.使用CDC Source从Postgresql读取数据
        DataStreamSource<String> pgds = env.addSource(pgsource, "data_source_01");
        pgds.print();
        env.execute();
        //4.打印数据
    }
}