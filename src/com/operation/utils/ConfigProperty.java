package com.operation.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigProperty extends Properties{		
	
	//S3
	private String bucketName;
	
	//Kinesis
	private String kinesisEndPoint;
	private String streamName;
	
	
	String configFile="/Aws.properties";
	
	public ConfigProperty(){
		try {
			load(this.getClass().getResourceAsStream(configFile));
			
		    setBucketName(getProperty("bucketName"));
		    
		    setStreamName(getProperty("streamName"));
		    
		    setStreamName(getProperty("kinesisEndpoint"));
		    

		} 
		catch (IOException ex) {
		    ex.printStackTrace();
		}		
	}

	/**
	 * @return the bucketName
	 */
	public String getBucketName() {
		return bucketName;
	}

	/**
	 * @param bucketName the bucketName to set
	 */
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	/**
	 * @return the streamName
	 */
	public String getStreamName() {
		return streamName;
	}

	/**
	 * @param streamName the streamName to set
	 */
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getKinesisEndPoint() {
		return kinesisEndPoint;
	}

	public void setKinesisEndPoint(String kinesisEndPoint) {
		this.kinesisEndPoint = kinesisEndPoint;
	}
	
	

}
