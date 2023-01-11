package main.java.yumchina.mysql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;


public class getmysql_cdc_mysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // 配置 Debezium在初始化快照的时候（扫描历史数据的时候） =》 不要锁表
        Properties properties = new Properties();
        properties.setProperty("debezium.snapshot.locking.mode", "none");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");

        env.setParallelism(1);
        TableResult tableResult = tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS `default_catalog`.`mysql`");
        TableResult createrSourcetable = tableEnv.executeSql("" +
                "create table source_orderdetails " +
                "("
                + " orderNumber int NOT NULL,"
                + " productCode string NOT NULL,"
                + " quantityOrdered int NOT NULL,"
                + " priceEach decimal NOT NULL,"
                + " orderLineNumber int NOT NULL,"
                + " PRIMARY KEY (`orderNumber`,`productCode`) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'mysql-cdc',"
                + "  'hostname' = 'localhost',"
                + "  'port' = '3306',"
                + "  'username' = 'mycdc',"
                + "  'password' = 'hirisun',"
                + "  'database-name' = 'classicmodels',"
                + "  'table-name' = 'orderdetails'"
                + ")");
        // execute SELECT statement
        TableResult Execsql = tableEnv.executeSql("select a.productCode,sum(a.quantityOrdered) qty , now() from source_orderdetails a" +
                " where a.productCode = 'S24_1785'" +
                " group by a.productCode");

        Execsql.print();
        env.execute("getmysql_cdc_mysql");

    }
}
