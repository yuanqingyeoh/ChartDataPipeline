package marketdata.function;

import marketdata.model.candle.Candle;
import marketdata.model.candle.Candle_M1;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CandleFunction extends ProcessAllWindowFunction<Candle_M1, Candle, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<Candle_M1, Candle, TimeWindow>.Context context, Iterable<Candle_M1> iterable, Collector<Candle> collector) throws Exception {
        Candle processingCandle = null;
        for (Candle candle : iterable) {
            if (processingCandle == null) {
                processingCandle = new Candle(candle.getSymbol(), candle.getTimestamp(), candle.getOpen(), candle.getHigh(), candle.getLow(), candle.getClose(), candle.getVolume());
            } else {
                // Sum the volume
                processingCandle.setVolume(processingCandle.getVolume() + candle.getVolume());

                // Set the close
                processingCandle.setClose(candle.getClose());

                // Process the Low
                processingCandle.setLow(processingCandle.getLow().min(candle.getLow()));

                // Process the High
                processingCandle.setHigh(processingCandle.getHigh().max(candle.getHigh()));
            }
        }

        collector.collect(processingCandle);
    }
}
