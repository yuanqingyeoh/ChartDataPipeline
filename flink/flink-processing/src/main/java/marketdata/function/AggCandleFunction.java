package marketdata.function;

import marketdata.model.Candle;
import marketdata.model.Candle_M1;
import marketdata.model.TickData;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class AggCandleFunction extends ProcessAllWindowFunction<Candle,Candle, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(AggCandleFunction.class);

    @Override
    public void process(ProcessAllWindowFunction<Candle, Candle, TimeWindow>.Context context, Iterable<Candle> iterable, Collector<Candle> collector) throws Exception {

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

            Map<Date, Candle> candleStore = new HashMap<>();
            for (Candle data: iterable) {
                // If new candle is processed later use that candle instead
                if (!candleStore.containsKey(data.getTimestamp()) || data.getProcessingTime().isAfter(candleStore.get(data.getTimestamp()).getProcessingTime())) {
                    candleStore.put(data.getTimestamp(), data);
                }
            }

            for (Candle data : candleStore.values()) {
                LOG.debug("Processing for window " + windowStart + " to " + windowEnd + " : " + data);

                // Set SYMBOL
                processingCandle.setSymbol(data.getSymbol());

                // Calculate volume
                processingCandle.setVolume(processingCandle.getVolume() + data.getVolume());

                // Calculate high
                if (processingCandle.getHigh() == null) {
                    processingCandle.setHigh(data.getHigh());
                } else {
                    processingCandle.setHigh(processingCandle.getHigh().max(data.getHigh()));
                }

                // Calculate low
                if (processingCandle.getLow() == null) {
                    processingCandle.setLow(data.getLow());
                } else {
                    processingCandle.setLow(processingCandle.getLow().min(data.getLow()));
                }

                // Calculate close
                if (data.getTimestamp().after(latest)) {
                    latest = data.getTimestamp();
                    processingCandle.setClose(data.getClose());
                }

                // Calculate open
                if (data.getTimestamp().before(earliest)) {
                    earliest = data.getTimestamp();
                    processingCandle.setOpen(data.getOpen());
                }

            }
            collector.collect(processingCandle);
        }
    }
}
