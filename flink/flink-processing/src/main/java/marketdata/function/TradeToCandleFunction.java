package marketdata.function;

import marketdata.model.candle.Candle;
import marketdata.model.TickData;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.TimeZone;

public class TradeToCandleFunction extends ProcessWindowFunction<TickData, Candle, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(TradeToCandleFunction.class);

    @Override
    public void process(String s, ProcessWindowFunction<TickData, Candle, String, TimeWindow>.Context context, Iterable<TickData> iterable, Collector<Candle> collector) throws Exception {

        LocalDateTime windowStart =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()),
                        TimeZone.getDefault().toZoneId());

        LocalDateTime windowEnd =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()),
                        TimeZone.getDefault().toZoneId());

        if (iterable.iterator().hasNext()) {

            Date earliest = new Date();
            Date latest = new Date(0);

            Candle processingCandle = new Candle();
            processingCandle.setTimestamp(new Date(context.window().getStart()));
            processingCandle.setVolume(0);
            processingCandle.setProcessingTime(Instant.now());
            // Set SYMBOL
            processingCandle.setSymbol(s);

            for (TickData tickData: iterable) {
                LOG.debug("Processing for window " + windowStart + " to " + windowEnd + " : " + tickData);



                // Calculate volume
                processingCandle.setVolume(processingCandle.getVolume() + tickData.getQuantity());

                // Calculate high
                if (processingCandle.getHigh() == null) {
                    processingCandle.setHigh(tickData.getPrice());
                } else {
                    processingCandle.setHigh(processingCandle.getHigh().max(tickData.getPrice()));
                }

                // Calculate low
                if (processingCandle.getLow() == null) {
                    processingCandle.setLow(tickData.getPrice());
                } else {
                    processingCandle.setLow(processingCandle.getLow().min(tickData.getPrice()));
                }

                // Calculate close
                if (tickData.getTimestamp().after(latest)) {
                    latest = tickData.getTimestamp();
                    processingCandle.setClose(tickData.getPrice());
                }

                // Calculate open
                if (tickData.getTimestamp().before(earliest)) {
                    earliest = tickData.getTimestamp();
                    processingCandle.setOpen(tickData.getPrice());
                }

            }
            collector.collect(processingCandle);
        }
    }
}
