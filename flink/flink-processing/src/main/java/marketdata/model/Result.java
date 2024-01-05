package marketdata.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class Result{

	@JsonProperty("data")
	private List<DataItem> data;

	@JsonProperty("instrument_name")
	private String instrumentName;

	@JsonProperty("channel")
	private String channel;

	@JsonProperty("subscription")
	private String subscription;

	public void setData(List<DataItem> data){
		this.data = data;
	}

	public List<DataItem> getData(){
		return data;
	}

	public void setInstrumentName(String instrumentName){
		this.instrumentName = instrumentName;
	}

	public String getInstrumentName(){
		return instrumentName;
	}

	public void setChannel(String channel){
		this.channel = channel;
	}

	public String getChannel(){
		return channel;
	}

	public void setSubscription(String subscription){
		this.subscription = subscription;
	}

	public String getSubscription(){
		return subscription;
	}

	@Override
	public String toString() {
		return "Result{" +
				"data=" + data +
				", instrumentName='" + instrumentName + '\'' +
				", channel='" + channel + '\'' +
				", subscription='" + subscription + '\'' +
				'}';
	}
}