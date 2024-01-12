package marketdata.jobs;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class BatchAPIJob {

    /**
     *  This Job used TableAPI to process the batch candle API data.
     */

    public static void main(String[] args) throws Exception {

        final String source_csv_ddl = "CREATE TEMPORARY TABLE candle_input (\n" +
                "                `interval` STRING,\n" +
                "                `instrument_name` String,\n" +
                "                `data` ARRAY<ROW<o STRING, h STRING, l STRING, c STRING, v STRING, t BIGINT>>\n" +
                "        ) WITH (\n" +
                "                'connector' = 'kafka',\n" +
                "                'topic' = 'xrp-usdt-batch',\n" +
                "                'format' = 'json',\n" +
                "                'properties.bootstrap.servers'='localhost:9092',\n" +
                "                'properties.group.id'='CRYPTO_XRP_USDT_BATCH_CONSUMER_FLINK',\n" +
                "                'scan.startup.mode'='earliest-offset'" +
                "        )";

        final String candle_data_ddl = "CREATE VIEW candle_data \n" +
                "   AS SELECT\n" +
                "    CAST(`o` AS DECIMAL(7,5)) AS `open`,\n" +
                "    CAST(`h` AS DECIMAL(7,5)) AS `high`,\n" +
                "    CAST(`l` AS DECIMAL(7,5)) AS `low`,\n" +
                "    CAST(`c` AS DECIMAL(7,5)) AS `close`,\n" +
                "    CAST(`v` AS INT) AS `volume`,\n" +
                "    CAST(`t` AS TIMESTAMP(3)) AS `timestamp`\n" +
                "FROM\n" +
                "    candle_input\n" +
                "CROSS JOIN UNNEST(data) as T(o, h, l, c, v, t);";

        final String out_csv_ddl = "CREATE TABLE candle_output (\n" +
                "                `symbol` STRING,\n" +
                "                `open` DECIMAL(7, 2),\n" +
                "                `high` DECIMAL(7, 2),\n" +
                "                `close` DECIMAL(7, 2),\n" +
                "                `low` DECIMAL(7, 2),\n" +
                "                `volume` INTEGER,\n" +
                "                `timestamp` TIMESTAMP\n "  +
                "        ) WITH (\n" +
                "                'connector' = 'filesystem',\n" +
                "                'path' = 'file:///C:/Self_Exploration/ChartData/out',\n" +
                "                'format' = 'csv',\n" +
                "                'sink.partition-commit.delay'='1 h',\n" +
                "                'sink.partition-commit.trigger'='partition-time',\n" +
                "                'sink.partition-commit.policy.kind'='success-file'" +
                "        )";

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
//                .inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Define the table
        tEnv.executeSql(source_csv_ddl);
        tEnv.executeSql(candle_data_ddl);

        tEnv.executeSql(out_csv_ddl);




        Table raw_table = tEnv.from("candle_data");

//        raw_table.execute().print();


        Table processed_table = raw_table;

        processed_table = processed_table.addColumns(lit("XRP_USDT").as("symbol"));

        processed_table.printSchema();

        Table result = processed_table
                .window(Tumble.over(lit(5).minutes()).on($("timestamp")).as("w")) // define window
                .groupBy($("symbol"), $("w")) // group by key and window
                // access window properties and aggregate
                .select(
                        $("symbol"),
                        $("open").firstValue().as("open"),
                        $("high").max().as("high"),
                        $("close").lastValue().as("close"),
                        $("low").min().as("low"),
                        $("volume").sum().as("volume"),
                        $("w").start().as("timestamp")
                );

        result.execute().print();

        TablePipeline tablePipeline = result.insertInto("candle_output");
        tablePipeline.printExplain();

        tablePipeline.execute().print();
    }

}
