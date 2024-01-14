package marketdata.model.candle;

import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.Date;

@Table(keyspace = "market_data", name = "TB_H1")
public class Candle_H1 extends Candle {

    public Candle_H1() {
        super();
    }

    public Candle_H1(Candle candle) {
        super(candle.getSymbol(), candle.getTimestamp(), candle.getOpen(), candle.getHigh(), candle.getLow(), candle.getClose(), candle.getVolume());
    }

    public Candle_H1(String symbol, Date timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, Integer volume) {
        super(symbol, timestamp, open, high, low, close, volume);
    }
}
