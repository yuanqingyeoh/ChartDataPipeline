package marketdata.model.candle;

import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.Date;

@Table(keyspace = "market_data", name = "TB_D")
public class Candle_D extends Candle {

    public Candle_D() {
        super();
    }

    public Candle_D(Candle candle) {
        super(candle.getSymbol(), candle.getTimestamp(), candle.getOpen(), candle.getHigh(), candle.getLow(), candle.getClose(), candle.getVolume());
    }

    public Candle_D(String symbol, Date timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, BigDecimal volume) {
        super(symbol, timestamp, open, high, low, close, volume);
    }
}