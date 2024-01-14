package marketdata.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.Date;

@Table(keyspace = "market_data", name = "TB_TICK")
public class TickData {

    @Column(name = "SYMBOL")
    private String symbol;

    @Column(name="TIMESTAMP")
    private Date timestamp;

    @Column(name = "PRICE")
    private BigDecimal price;

    @Column(name = "QUANTITY")
    private BigDecimal quantity;

    public TickData() {
    }

    public TickData(String symbol, Date timestamp, BigDecimal price, BigDecimal quantity) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
        this.quantity = quantity;
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

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "TickData{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", price=" + price +
                ", quantity=" + quantity +
                '}';
    }
}
