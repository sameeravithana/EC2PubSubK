package com.pubsub.model;

import com.operation.utils.NanoClock;

public class Publication {
	
	private String resource;
	private String referee;
	private long timestamp;
	
	public Publication(){
		
	}
	
	public Publication(String resource,String referee){
		this.resource=resource;
		this.referee=referee;
		timestamp=new NanoClock().getTime();
	}
	
	@Override
    public int hashCode() {
        int result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + (referee != null ? referee.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Publication{" +
                "resource='" + resource + '\'' +
                ", referee='" + referee + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getResource() {
		return resource;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public String getReferee() {
		return referee;
	}

	public void setReferee(String referee) {
		this.referee = referee;
	}

	

}
