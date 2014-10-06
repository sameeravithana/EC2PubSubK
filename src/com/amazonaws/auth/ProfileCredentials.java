package com.amazonaws.auth;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class ProfileCredentials extends ProfileCredentialsProvider {
	
	static String profileName = "GeekTube405";

	public ProfileCredentials() {
		super(profileName);
	}
	
	public AWSCredentials getCredentials() throws AmazonClientException{
		//return new ClasspathPropertiesFileCredentialsProvider().getCredentials();
		return super.getCredentials();
	}
	
	public AmazonSQSClient getSQSClient(){
		return new AmazonSQSClient(getCredentials());
	}
	
	public AmazonSNSClient getSNSClient(){
		return new AmazonSNSClient(getCredentials());
	}
	
	public AmazonEC2Client getEC2Client(){
		return new AmazonEC2Client(getCredentials());
	}
	
	public AmazonS3Client getS3Client(){
		return new AmazonS3Client(getCredentials());
	}
	
	public AmazonDynamoDBClient getDynamoDBClient(){
		return new AmazonDynamoDBClient(getCredentials());
	}
	
	public AmazonKinesisClient getKinesisClient(){
		return new AmazonKinesisClient(getCredentials());
	}
	
	

}
