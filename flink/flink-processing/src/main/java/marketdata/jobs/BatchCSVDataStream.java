package marketdata.jobs;

import marketdata.function.CandleFunction;
import marketdata.model.OHCLVInput;
import marketdata.model.Candle_M1;
import marketdata.model.Candle_M5;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchCSVDataStream {

    private static final String PATH = "C:/Self_Exploration/ChartData/data/XAUUSD_2022_all.csv";
//    private static final String PATH = "./data/XAUUSD_2022_all.csv";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, whicE is the main entry point
        // to building Flink applications.

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();


        File file = new File(PATH);
        final String SYMBOL = "XAUUSD";

        CsvReaderFormat<OHCLVInput> csvFormat = CsvReaderFormat.forPojo(OHCLVInput.class);
//        FileSource<OHCLVInput> source =
//                FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(file)).build();

        BulkFormat<OHCLVInput, FileSourceSplit> bulkFormat =
                new StreamFormatAdapter<>(CsvReaderFormat.forPojo(OHCLVInput.class));
        FileSource<OHCLVInput> source =
                FileSource.forBulkFileFormat(bulkFormat, Path.fromLocalFile(file)).build();

        // M1 processing (Convert to OHCLV format)
        DataStream<Candle_M1> data = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "CSV Source")
                .flatMap(new FlatMapFunction<OHCLVInput, Candle_M1>() {
                    @Override
                    public void flatMap(OHCLVInput streamInput, Collector<Candle_M1> collector) throws Exception {
                        if (StringUtils.isNotBlank(streamInput.getDate()) || StringUtils.isNotBlank(streamInput.getTime())) {
                            collector.collect(new Candle_M1(SYMBOL,
                                    convertToTimestamp(streamInput.getDate(), streamInput.getTime()),
                                    streamInput.getOpen(),
                                    streamInput.getHigh(),
                                    streamInput.getLow(),
                                    streamInput.getClose(),
                                    streamInput.getVolume()));
                        }
                    }
                });


        data.print("Candle M1");

        // M5 processing
        DataStream<Candle_M5> data_m5 = data
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Candle_M1>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new CandleFunction())
                .map(Candle_M5::new);

        data_m5.print("Candle M5");

//        CassandraSink.addSink(data)
////                .setHost("cassandra", 9042)
//                .setHost("127.0.0.1", 9042)
//                .setDefaultKeyspace("market_data")
//                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
//                .build();

        // Execute program, beginning computation.
        env.execute("Process CSV data. Process M1, M5");
    }

    public static Timestamp convertToTimestamp(String dateString, String timeString) throws ParseException {

        if (StringUtils.isBlank(dateString) || StringUtils.isBlank(timeString)) {
            return null;
        }

        // Create a combined date-time string in the format "yyyy.MM.dd HH:mm"
        String dateTimeString = dateString + " " + timeString;

        // Create a SimpleDateFormat for the combined format
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm");

        // Parse the combined date-time string to a Date object
        Date date = dateFormat.parse(dateTimeString);

        // Convert the Date object to a Timestamp
        return new Timestamp(date.getTime());
    }
}
