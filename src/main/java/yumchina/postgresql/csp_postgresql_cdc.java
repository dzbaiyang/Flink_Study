package main.java.yumchina.postgresql;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import com.starrocks.connector.flink.StarRocksSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;




public class csp_postgresql_cdc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // 配置 Debezium在初始化快照的时候（扫描历史数据的时候） =》 不要锁表
        Properties properties = new Properties();
        properties.setProperty("debezium.snapshot.locking.mode", "none");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
        configuration.setString("table.exec.sink.not-null-enforcer", "drop");
        //        tableEnv.executeSql("set table.exec.sink.not-null-enforcer=drop");

        env.setParallelism(1);


        TableResult tableResult = tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS `default_catalog`.`postgres`");
        TableResult tableResult2 = tableEnv.executeSql("" +
                "CREATE TABLE IF NOT EXISTS `default_catalog`.`postgres`.`public__cs_incident_202210_src` (" +
                "  `id` STRING NOT NULL," +
                "  `incident_no` BIGINT NOT NULL," +
                "  `type` STRING NOT NULL," +
                "  `process_type` STRING NULL," +
                "  `tag_id` STRING NULL," +
                "  `custom_tag` STRING NULL," +
                "  `status` STRING NOT NULL," +
                "  `assign` STRING NULL," +
                "  `complete_type` STRING NULL," +
                "  `activity_id` STRING NULL," +
                "  `original_incident_id` STRING NULL," +
                "  `source` STRING NOT NULL," +
                "  `source_id` STRING NULL," +
                "  `source_user_code` STRING NULL," +
                "  `incident_date` TIMESTAMP NOT NULL," +
                "  `incident_details` STRING NOT NULL," +
                "  `dining_way` STRING NOT NULL," +
                "  `business_unit_id` STRING NOT NULL," +
                "  `market_id` STRING NULL," +
                "  `state_id` STRING NULL," +
                "  `location_id` STRING NULL," +
                "  `dept_id` STRING NULL," +
                "  `dept_no` STRING NULL," +
                "  `img_urls` STRING NULL," +
                "  `customer_name` STRING NOT NULL," +
                "  `customer_telephone` STRING NOT NULL," +
                "  `order_details` STRING NULL," +
                "  `addition_params` STRING NULL," +
                "  `timeline` STRING NULL," +
                "  `create_date` TIMESTAMP NOT NULL," +
                "  `create_by` STRING NOT NULL," +
                "  `update_date` TIMESTAMP NOT NULL," +
                "  `update_by` STRING NOT NULL," +
                "  `delete_flag` INT NULL," +
                "  `warned` BOOLEAN NULL," +
                "  `warn_date` TIMESTAMP NULL," +
                "  `priority` STRING NULL," +
                "  `pre_tag` STRING NULL," +
                "  `proxy_source` STRING NULL," +
                "  `kp_key_ids` STRING NULL," +
                "  PRIMARY KEY(`id`)" +
                " NOT ENFORCED" +
                ") with (" +
                "  'connector' = 'postgres-cdc'," +
                "  'port' = '1922'," +
                "  'password' = '123@prod'," +
                "  'table-name' = 'cs_incident_202210'," +
                "  'hostname' = '172.25.242.245'," +
                "  'username' = 'developer'," +
                "  'database-name' = 'csp_prod_cs'," +
                "  'schema-name' = 'public'," +
                "  'debezium.slot.name' = 'cs_incident_202210'," +
                "  'decoding.plugin.name' = 'wal2json'" +
                ")");

        TableResult tableResult3 = tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `default_catalog`.`postgres`.`public__cs_incident_202210_sink` (" +
                "  `id` STRING NOT NULL," +
                "  `incident_no` BIGINT NOT NULL," +
                "  `type` STRING NOT NULL," +
                "  `process_type` STRING NULL," +
                "  `tag_id` STRING NULL," +
                "  `custom_tag` STRING NULL," +
                "  `status` STRING NOT NULL," +
                "  `assign` STRING NULL," +
                "  `complete_type` STRING NULL," +
                "  `activity_id` STRING NULL," +
                "  `original_incident_id` STRING NULL," +
                "  `source` STRING NOT NULL," +
                "  `source_id` STRING NULL," +
                "  `source_user_code` STRING NULL," +
                "  `incident_date` TIMESTAMP NOT NULL," +
                "  `incident_details` STRING NOT NULL," +
                "  `dining_way` STRING NOT NULL," +
                "  `business_unit_id` STRING NOT NULL," +
                "  `market_id` STRING NULL," +
                "  `state_id` STRING NULL," +
                "  `location_id` STRING NULL," +
                "  `dept_id` STRING NULL," +
                "  `dept_no` STRING NULL," +
                "  `img_urls` STRING NULL," +
                "  `customer_name` STRING NOT NULL," +
                "  `customer_telephone` STRING NOT NULL," +
                "  `order_details` STRING NULL," +
                "  `addition_params` STRING NULL," +
                "  `timeline` STRING NULL," +
                "  `create_date` TIMESTAMP NOT NULL," +
                "  `create_by` STRING NOT NULL," +
                "  `update_date` TIMESTAMP NOT NULL," +
                "  `update_by` STRING NOT NULL," +
                "  `delete_flag` INT NULL," +
                "  `warned` BOOLEAN NULL," +
                "  `warn_date` TIMESTAMP NULL," +
                "  `priority` STRING NULL," +
                "  `pre_tag` STRING NULL," +
                "  `proxy_source` STRING NULL," +
                "  `kp_key_ids` STRING NULL," +
                "  PRIMARY KEY(`id`)" +
                " NOT ENFORCED" +
                ") with (" +
                "  'load-url' = '172.25.201.99:8030'," +
                "  'database-name' = 'csp'," +
                "  'sink.max-retries' = '10'," +
                "  'sink.properties.column_separator' = '\\x01'," +
                "  'sink.properties.row_delimiter' = '\\x02'," +
                "  'sink.properties.format' = 'json'," +
                "  'sink.properties.strip_outer_array' = 'true'," +
                "  'table-name' = 'cs_incident'," +
                "  'username' = 'root'," +
                "  'password' = 'fF0&cD2@bS0#'," +
                "  'jdbc-url' = 'jdbc:mysql://172.25.201.99:9030'," +
                "  'sink.buffer-flush.interval-ms' = '15000'," +
                "  'connector' = 'starrocks'" +
                ")");

        TableResult tableResult4 = tableEnv.executeSql("INSERT INTO `default_catalog`.`postgres`.`public__cs_incident_202210_sink` " +
                "SELECT * FROM `default_catalog`.`postgres`.`public__cs_incident_202210_src`");

        tableResult4.print();
        env.execute("csp_postgresql_cdc");
    }
}