package com.amazonaws.compute;

import java.net.UnknownHostException;
import java.util.List;

import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.compute.kinesis.KinesisDriver;
import com.amazonaws.services.sns.util.Topics;

public class Engine {
	
		
	SQSDriver sqs_drv;
	public SQSDriver getSqs_drv() {
		return sqs_drv;
	}


	SNSDriver sns_drv;	
	public SNSDriver getSns_drv() {
		return sns_drv;
	}

	EC2Driver ec2_drv;    
    public EC2Driver getEc2_drv() {
		return ec2_drv;
	}
    
    S3Driver s3_drv;
    public S3Driver getS3_drv() {
		return s3_drv;
	}
    

	ProfileCredentials profile;
    
    String topicArn;

	private KinesisDriver kin_drv;

	public KinesisDriver getKin_drv() {
		return kin_drv;
	}
	
	public String getTopicArn() {
		return topicArn;
	}


	public void setTopicArn(String snsTopic) {
		this.topicArn = sns_drv.createSNSTopic(snsTopic);
	}


	public Engine() throws UnknownHostException{
		sqs_drv=new SQSDriver();
		sns_drv=new SNSDriver();
		ec2_drv=new EC2Driver();
		kin_drv=new KinesisDriver();
		s3_drv=new S3Driver();
		profile=new ProfileCredentials();
		
	
		
	}
	
	
	public void runSQNS(String snsTopic){		
		
		// create a topic
		String topicArn=sns_drv.createSNSTopic(snsTopic);
		
		List<String> listQueues=sqs_drv.listEC2Queues();
        
        String myQueueUrl= listQueues.get(0);
        System.out.println(myQueueUrl);
		
        // custom security policy to allow the topic to deliver messages to the queue
		Topics.subscribeQueue(sns_drv.getSns(), sqs_drv.getSqs(), topicArn, myQueueUrl);
        
        //String myQueueUrl = sqs_drv.createEC2Queue("EC2Queue");

        

        //String send_message = "Hi, EC2 SQS!";
        //sqs_drv.sendMessage(myQueueUrl, send_message);     
       
        
        //String messageRecieptHandle = messages.get(0).getReceiptHandle();
        //sqs_drv.deleteMessage(myQueueUrl, messageRecieptHandle);

        
        //sqs_drv.deleteEC2Queue(myQueueUrl); 
        
        //String topicArn="arn:aws:sns:us-east-1:511368698353:EC2SNS";
        //String protocol="sqs";
        //String endPoint="arn:aws:sqs:us-east-1:511368698353:EC2Queue";
        //sns_drv.subscribe(topicArn,protocol,endPoint);
        
        //String msg="Loila! Amazon AWS.."+new Date();
        //sns_drv.publish(topicArn, msg);
        
        //List<Message> messages = sqs_drv.receiveMessage(myQueueUrl);
        //sqs_drv.listReceivedMessages(messages);
	
	}
	
	



	


	


	

}
