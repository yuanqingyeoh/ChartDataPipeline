package marketdata.model.candle;

import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.Date;

@Table(keyspace = "market_data", name = "TB_M15")
public class Candle_M15 extends Candle {

    public Candle_M15() {
        super();
    }

    public Candle_M15(Candle candle) {
        super(candle.getSymbol(), candle.getTimestamp(), candle.getOpen(), candle.getHigh(), candle.getLow(), candle.getClose(), candle.getVolume());
    }

    public Candle_M15(String symbol, Date timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, Integer volume) {
        super(symbol, timestamp, open, high, low, close, volume);
    }
}
