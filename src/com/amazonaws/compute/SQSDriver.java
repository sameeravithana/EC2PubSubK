package com.amazonaws.compute;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

//import org.apache.log4j.Logger;
//import org.apache.log4j.BasicConfigurator;






import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSDriver {

	static SQSDriver sqsDriver;
	
	AmazonSQS sqs;
	public AmazonSQS getSqs() {
		return sqs;
	}

	Region region;
	
   // Define a static logger variable so that it references the
   // Logger instance named "Driver".
   //static Logger logger = Logger.getLogger(Driver.class);
	
	public SQSDriver(){
		
        sqs = new ProfileCredentials().getSQSClient();
        region = Region.getRegion(Regions.US_EAST_1);
        sqs.setRegion(region);
        
        // Set up a simple configuration that logs on the console.
        //BasicConfigurator.configure();
	}
	
	// Create a queue
	public String createEC2Queue(String qname){
		System.out.println("Creating EC2 Queue: "+ qname);
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qname);
        String queueUrl = getSqs().createQueue(createQueueRequest).getQueueUrl();
        return queueUrl;
	}
	
	// List queues
	public List<String> listEC2Queues(){
		System.out.println("Listing all queues in your account.");
		List<String> queueList = new LinkedList<String>();
		for (String queueUrl : getSqs().listQueues().getQueueUrls()) {
            System.out.println("  QueueUrl: " + queueUrl);
            queueList.add(queueUrl);
        }
		return queueList;
	}
	
	public int getQueueCount(){
		return listEC2Queues().size();
	}
	
	public void sendMessage(String queueUrl,String message){
		System.out.println("Sending a message to the queue "+queueUrl+"....");
		System.out.println("Message: "+message);
		
		try{
			SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, message);
			getSqs().sendMessage(sendMessageRequest);
		}catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
        	System.out.println("Error Message: " + ace.getMessage());
        }
		System.out.println("Sent message.");
	}
	
	public List<Message> receiveMessage(String queueUrl){
		System.out.println("Receiving a message from the queue "+queueUrl+"....");
		List<Message> messages = null;
		try{
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		}catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
        	System.out.println("Error Message: " + ace.getMessage());
        }
		return messages;
	}
	
	public void listReceivedMessages(List<Message> messages){
		for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();
	}
	
	// Delete a message
	public void deleteMessage(String queueUrl, String messageRecieptHandle){
		System.out.println("Deleting a message from the queue "+queueUrl+"....");
		
		try{
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, messageRecieptHandle);
	        getSqs().deleteMessage(deleteMessageRequest);
		}catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
        	System.out.println("Error Message: " + ace.getMessage());
        }		
		
	}
	
	// Delete a queue
	public void deleteEC2Queue(String queueUrl){
		System.out.println("Deleting the queue "+queueUrl+"....");
		
		try{
			DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueUrl);
	        getSqs().deleteQueue(deleteQueueRequest);
		}catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
        	System.out.println("Error Message: " + ace.getMessage());
        }		
		
		}
	
	public static synchronized SQSDriver getSQSDriver(){
        if(sqsDriver == null){
                  try{
                	  sqsDriver = new SQSDriver();
                  }
                  catch(Exception e){
                           e.printStackTrace();
                  }
        }
        return sqsDriver;
	}
	
	
	

}
