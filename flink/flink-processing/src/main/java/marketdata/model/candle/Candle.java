package marketdata.model.candle;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Transient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

public class Candle {

    @Column(name = "SYMBOL")
    String symbol;
    @Column(name="TIMESTAMP")
    Date timestamp;
    @Column(name="OPEN")
    BigDecimal open;
    @Column(name="HIGH")
    BigDecimal high;
    @Column(name="LOW")
    BigDecimal low;
    @Column(name="CLOSE")
    BigDecimal close;
    @Column(name="VOLUME")
    BigDecimal volume;
    @Transient
    Instant processingTime;

    public Candle() {
    }

    public Candle(String symbol, Date timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, BigDecimal volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public void setOpen(BigDecimal open) {
        this.open = open;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public void setHigh(BigDecimal high) {
        this.high = high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public void setLow(BigDecimal low) {
        this.low = low;
    }

    public BigDecimal getClose() {
        return close;
    }

    public void setClose(BigDecimal close) {
        this.close = close;
    }

    public BigDecimal getVolume() {
        return volume;
    }

    public void setVolume(BigDecimal volume) {
        this.volume = volume;
    }

    public Instant getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(Instant processingTime) {
        this.processingTime = processingTime;
    }

    @Override
    public String toString() {
        return "Candle{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", volume=" + volume +
                ", processingTime=" + processingTime +
                '}';
    }
}
