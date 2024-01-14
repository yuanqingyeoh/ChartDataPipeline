package marketdata.model.candle;

import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.Date;

@Table(keyspace = "market_data", name = "TB_M1")
public class Candle_M1 extends Candle {

    public Candle_M1() {
        super();
    }

    public Candle_M1(Candle candle) {
        super(candle.getSymbol(), candle.getTimestamp(), candle.getOpen(), candle.getHigh(), candle.getLow(), candle.getClose(), candle.getVolume());
    }

    public Candle_M1(String symbol, Date timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, Integer volume) {
        super(symbol, timestamp, open, high, low, close, volume);
    }
}
