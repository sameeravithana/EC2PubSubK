package com.amazonaws.compute;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.GetEndpointAttributesRequest;
import com.amazonaws.services.sns.model.GetPlatformApplicationAttributesRequest;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class SNSDriver {
	
	static SNSDriver snsDriver;
	
	AWSCredentials credentials;
	public AWSCredentials getCredentials() {
		return credentials;
	}

	AmazonSNSClient sns;
	public AmazonSNSClient getSns() {
		return sns;
	}

	AmazonSQSClient sqs;	
	public AmazonSQSClient getSqs() {
		return sqs;
	}

	Region region;
	
	public SNSDriver(){
		
		sns = new ProfileCredentials().getSNSClient();
		sqs = new ProfileCredentials().getSQSClient();
        region = Region.getRegion(Regions.US_EAST_1);
        sns.setRegion(region);
	}
	
	
	public String createSNSTopic(String topic){		
			System.out.println("Creating SNS Topic: "+ topic);
			//create a new SNS topic
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topic).withName(topic);
							
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			//print TopicArn
			System.out.println(createTopicResult);
			//get request id for CreateTopicRequest from SNS metadata		
			System.out.println("CreateTopicRequest - " + sns.getCachedResponseMetadata(createTopicRequest));
	        return createTopicResult.getTopicArn();	
	}
	
	// List 100 topics
	public List<String> listEC2Topics(){
		ListTopicsRequest listTopicsRequest = new ListTopicsRequest();
		ListTopicsResult listTopicsResult = sns.listTopics(listTopicsRequest);
		List<String> topicList = new LinkedList<String>();
		for (Topic topic : listTopicsResult.getTopics()) {
			String _topic=topic.getTopicArn();
            System.out.println("  Topic: " + _topic);
            topicList.add(_topic);
        }
		return topicList;
		
	}
	
	
	public int getTopicCount(){
		return listEC2Topics().size();
	}
	
		
	public String subscribe(String topicArn, String protocol, String endPoint){
		if (protocol.equalsIgnoreCase("email")) {
			// subscribe to an SNS topic
			SubscribeRequest subRequest = new SubscribeRequest(topicArn,
					protocol, endPoint);

			SubscribeResult subResult = sns.subscribe(subRequest);
			// get request id for SubscribeRequest from SNS metadata
			System.out.println("SubscribeRequest - "
					+ sns.getCachedResponseMetadata(subRequest));

			String subArn = subResult.getSubscriptionArn();
			System.out.println("SubscribeResult - "
					+ sns.getCachedResponseMetadata(subRequest));
			System.out.println("Check your email and confirm subscription.");
			return subArn;
		}
		if(protocol.equalsIgnoreCase("sqs")){
			return this.suscribeSQS(getSns(), getSqs(), topicArn, endPoint);
		}
		return null;
	}	
	
	public String suscribeSQS(AmazonSNS sns, AmazonSQS sqs, String snsTopicArn, String sqsQueueUrl){
		Topics topic=new Topics();
		return topic.subscribeQueue(sns, sqs, snsTopicArn, sqsQueueUrl);
	}
	
	public String confirmSubscription(String topicArn, String token){
        ConfirmSubscriptionRequest request = new ConfirmSubscriptionRequest(topicArn, token);
        ConfirmSubscriptionResult result = sns.confirmSubscription(request);
        return result.getSubscriptionArn();
	}
	
	public String publish(String topicArn, String msg){
		//publish to an SNS topic
		//String msg = "My text published to SNS topic with email endpoint";
		
		Map<String, MessageAttributeValue> messageAttributes=new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("message", new MessageAttributeValue().withStringValue(msg));
			messageAttributes.put("timestamp", new MessageAttributeValue().withStringValue(new Date().toString()) );
			//messageAttributes.put("flag", new MessageAttributeValue().)
			
			
		PublishRequest publishRequest = new PublishRequest(topicArn, msg);//.withMessageAttributes(messageAttributes);
		PublishResult publishResult = sns.publish(publishRequest);
		//print MessageId of message published to SNS topic
		String messageId=publishResult.getMessageId();
		System.out.println("MessageId - " + messageId);
		return messageId;
	}
	

	
	public void publish(String msg){
		System.out.println("Under Development");
	}
	
	public void deleteSNSTopic(String topicArn){
		//delete an SNS topic
		DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(topicArn);
		sns.deleteTopic(deleteTopicRequest);
		//get request id for DeleteTopicRequest from SNS metadata
		System.out.println("DeleteTopicRequest - " + sns.getCachedResponseMetadata(deleteTopicRequest));
	}
	
	public static synchronized SNSDriver getSNSDriver(){
        if(snsDriver == null){
                  try{
                	  snsDriver = new SNSDriver();
                  }
                  catch(Exception e){
                           e.printStackTrace();
                  }
        }
        return snsDriver;
	}
	

}


