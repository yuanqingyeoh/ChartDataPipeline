package marketdata.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class DataItem{

	@JsonProperty("p")
	private String p;

	@JsonProperty("q")
	private String q;

	@JsonProperty("s")
	private String s;

	@JsonProperty("t")
	private long t;

	@JsonProperty("d")
	private String d;

	@JsonProperty("i")
	private String i;

	public void setP(String p){
		this.p = p;
	}

	public String getP(){
		return p;
	}

	public void setQ(String q){
		this.q = q;
	}

	public String getQ(){
		return q;
	}

	public void setS(String s){
		this.s = s;
	}

	public String getS(){
		return s;
	}

	public void setT(long t){
		this.t = t;
	}

	public long getT(){
		return t;
	}

	public void setD(String d){
		this.d = d;
	}

	public String getD(){
		return d;
	}

	public void setI(String i){
		this.i = i;
	}

	public String getI(){
		return i;
	}

	@Override
	public String toString() {
		return "DataItem{" +
				"price='" + p + '\'' +
				", quantity='" + q + '\'' +
				", side='" + s + '\'' +
				", timestamp=" + t +
				", tradeId='" + d + '\'' +
				", instrument_name='" + i + '\'' +
				'}';
	}
}