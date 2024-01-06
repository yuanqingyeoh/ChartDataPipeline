package marketdata.jobs;

import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

public class BatchCsvJob {

    /**
     *  This Job used TableAPI to process the input csv file.
     */

    public static void main(String[] args) throws Exception {

        final String source_csv_ddl = "CREATE TEMPORARY TABLE candle_input (\n" +
                "                `date` STRING,\n" +
                "                `time` STRING,\n" +
                "                `open` DECIMAL(7, 2),\n" +
                "                `high` DECIMAL(7, 2),\n" +
                "                `close` DECIMAL(7, 2),\n" +
                "                `low` DECIMAL(7, 2),\n" +
                "                `volume` INTEGER,\n" +
                "                `timestamp` AS TO_TIMESTAMP(CONCAT_WS(' ', `date`, `time`), 'yyyy.MM.dd HH:mm'),\n" +
                "                WATERMARK FOR `timestamp` AS `timestamp`\n" +
                "        ) WITH (\n" +
                "                'connector' = 'filesystem',\n" +
                "                'path' = 'file:///C:/Self_Exploration/ChartData/data/XAUUSD_2022_all.csv',\n" +
                "                'format' = 'csv',\n" +
                "                'csv.ignore-parse-errors' = 'true',\n" +
                "                'csv.allow-comments' = 'true'\n" +
                "        )";

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
        tEnv.executeSql(out_csv_ddl);

        Table raw_table = tEnv.from("candle_input");
        raw_table.printSchema();

        Table processed_table = raw_table;

        // Drop date and time column
        processed_table = processed_table.dropColumns($("date"), $("time"));

        processed_table = processed_table.addColumns(lit("XAUUSD").as("symbol"));

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


        TablePipeline tablePipeline = result.insertInto("candle_output");
        tablePipeline.printExplain();

        tablePipeline.execute().print();
    }

}
