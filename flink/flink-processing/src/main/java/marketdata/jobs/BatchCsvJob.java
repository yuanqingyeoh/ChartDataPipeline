package marketdata.jobs;

import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

public class BatchCsvJob {

    /**
     *  This Job used TableAPI to process the input csv file.
     */

    public static void main(String[] args) throws Exception {

        final String source_csv_ddl = "CREATE TEMPORARY TABLE ohclv_input (\n" +
                "                `date` STRING,\n" +
                "                `time` STRING,\n" +
                "                `open` DECIMAL(7, 2),\n" +
                "                `high` DECIMAL(7, 2),\n" +
                "                `close` DECIMAL(7, 2),\n" +
                "                `low` DECIMAL(7, 2),\n" +
                "                `volume` INTEGER\n" +
                "        ) WITH (\n" +
                "                'connector' = 'filesystem',\n" +
                "                'path' = 'file:///C:/Self_Exploration/ChartData/XAUUSD/XAUUSD_2022_all.csv',\n" +
                "                'format' = 'csv',\n" +
                "                'csv.ignore-parse-errors' = 'true',\n" +
                "                'csv.allow-comments' = 'true'" +
                "        )";

        final String out_csv_ddl = "CREATE TABLE ohclv_output (\n" +
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
                //.inStreamingMode()
                .inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Define the table
        tEnv.executeSql(source_csv_ddl);
        tEnv.executeSql(out_csv_ddl);

        // Another way to execute query
//        TableResult tableResult = tEnv.executeSql("SELECT  * FROM ohclv_input LIMIT 10");
//        tableResult.print();

        Table raw_table = tEnv.from("ohclv_input");

        // Create a string timestamp column
        Table processed_table = raw_table.addColumns(concatWs(" ", $("date"), $("time")).as("timestamp"));
        // Drop date and time column
        processed_table = processed_table.dropColumns($("date"), $("time"));
        // Convert timestamp string to timestamp type
        processed_table = processed_table.addOrReplaceColumns(toTimestamp($("timestamp"), "yyyy.MM.dd HH:mm").as("timestamp"));

        TablePipeline tablePipeline = processed_table.insertInto("ohclv_output");
        tablePipeline.printExplain();

        TableResult tableResult = tablePipeline.execute();
        tableResult.print();
    }



}
