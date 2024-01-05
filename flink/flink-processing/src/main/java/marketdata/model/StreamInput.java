package marketdata.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class StreamInput {

	@JsonProperty("result")
	private Result result;

	@JsonProperty("code")
	private int code;

	@JsonProperty("method")
	private String method;

	@JsonProperty("id")
	private int id;

	public void setResult(Result result){
		this.result = result;
	}

	public Result getResult(){
		return result;
	}

	public void setCode(int code){
		this.code = code;
	}

	public int getCode(){
		return code;
	}

	public void setMethod(String method){
		this.method = method;
	}

	public String getMethod(){
		return method;
	}

	public void setId(int id){
		this.id = id;
	}

	public int getId(){
		return id;
	}

	@Override
	public String toString() {
		return "StreamInput{" +
				"result=" + result +
				", code=" + code +
				", method='" + method + '\'' +
				", id=" + id +
				'}';
	}
}