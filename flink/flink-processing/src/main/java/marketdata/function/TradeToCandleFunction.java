package marketdata.function;

import marketdata.model.Candle;
import marketdata.model.TickData;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TradeToCandleFunction extends ProcessAllWindowFunction<TickData, Candle, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(TradeToCandleFunction.class);

    @Override
    public void process(ProcessAllWindowFunction<TickData, Candle, TimeWindow>.Context context, Iterable<TickData> iterable, Collector<Candle> collector) throws Exception {


        LOG.info("Starting process function...");
        if (iterable.iterator().hasNext()) {

            Date earliest = new Date();
            Date latest = new Date(0);

            Candle processingCandle = new Candle();
            processingCandle.setTimestamp(new Date(context.window().getStart()));
            processingCandle.setVolume(0);

            for (TickData tickData: iterable) {
                LOG.info("Processing: " + tickData);

                // Set SYMBOL
                processingCandle.setSymbol(tickData.getSymbol());

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
