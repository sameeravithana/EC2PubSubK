package com.amazonaws.compute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

public class KinesisDriver {
	
	AmazonKinesisClient kin;
	private String dfilePath="http://localhost:8080/EC2PubSub/datasets/nestoria-london.txt";
	
	S3Driver s3_drv;
	
	private static final Log LOG = LogFactory.getLog(KinesisDriver.class);

	public AmazonKinesisClient getKin() {
		return kin;
	}

	public KinesisDriver(){
		kin=new ProfileCredentials().getKinesisClient();
		s3_drv=new S3Driver();
			//kin.setEndpoint("us-east-1", "kinesis", "http://kinesis.us-east-1.amazonaws.com");
	}
	
	public void createStream(String streamName, int streamSize) {
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(streamName);
		createStreamRequest.setShardCount(streamSize);

		getKin().createStream(createStreamRequest);
		// The stream is now being created.
        LOG.info("Creating Stream : " + streamName);
        
		waitForStreamToBecomeAvailable(streamName);
		
	}
	
	private void waitForStreamToBecomeAvailable(String myStreamName) {

        System.out.println("Waiting for " + myStreamName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 20);
            } catch (InterruptedException e) {
                // Ignore interruption (doesn't impact stream creation)
            }
            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = getKin().describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.println("  - current state: " + streamStatus);
                if (streamStatus.equals("ACTIVE")) {
                    return;
                }
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) {
                    throw ase;
                }
                throw new RuntimeException("Stream " + myStreamName + " never went active");
            }
        }
    }
	
	
	
	public List<String> listKinesisStream() {
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(20);
		ListStreamsResult listStreamsResult = getKin().listStreams(
				listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();

		while (listStreamsResult.getHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames
						.get(streamNames.size() - 1));
			}
			listStreamsResult = getKin().listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());
		}
		
		return streamNames;
	}
	
	public String addDataRecords(String streamName, String _dataurl) throws IOException{		
		String lastSequenceNumber =null;
		PutRecordResult putRecordResult=null;
		
		//if(!_dataurl.isEmpty()) dfilePath=_dataurl;
		
		// Create a URL for the desired page
	    //URL url = new URL(dfilePath);
	    
	    /*elasticbeanstalk-us-east-1-511368698353 resources/datasets/clustered-clustered.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/clustered-uniform-half.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/clustered-uniform.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/faces.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/greek-cities.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/nestoria-london.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/small.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/uniform-clustered.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/uniform-uniform.txt
	    elasticbeanstalk-us-east-1-511368698353 resources/datasets/world-cities.txt*/
	    //String okey="resources/datasets/small.txt";
	    InputStream input=s3_drv.getObjectInput(_dataurl);
	    
	    BufferedReader in = new BufferedReader(new InputStreamReader(input));
		String line;
		int j=0;
		while ((line = in.readLine()) != null) {
			PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(streamName);
            putRecordRequest.setData(ByteBuffer.wrap(line.getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", j));
            putRecordResult = getKin().putRecord(putRecordRequest);
            System.out.println("Successfully putrecord, Data : " +line+ " partition key : " + putRecordRequest.getPartitionKey()
                    + ", ShardID : " + putRecordResult.getShardId());
            j++;
		}
		in.close();
		
		/*// Write 10 records to the stream
        for (int j = 0; j < 10; j++) {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(streamName);
            putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d", j).getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", j));
            putRecordResult = getKin().putRecord(putRecordRequest);
            System.out.println("Successfully putrecord, partition key : " + putRecordRequest.getPartitionKey()
                    + ", ShardID : " + putRecordResult.getShardId());
        }*/
		
		lastSequenceNumber=putRecordResult.getSequenceNumber();
		return lastSequenceNumber;
	}
	
	public String getShards(String streamName, Shard shard){
		String shardIterator;
		  GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		  getShardIteratorRequest.setStreamName(streamName);
		  getShardIteratorRequest.setShardId(shard.getShardId());
		  getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");//FIFO

		  GetShardIteratorResult getShardIteratorResult = getKin().getShardIterator(getShardIteratorRequest);
		  shardIterator = getShardIteratorResult.getShardIterator(); 
		  return shardIterator;
	}
	
	public List<Record> getDataRecords(String streamName, Shard shard) {
		// Continuously read data records from a shard
		List<Record> records;
		String shardIterator;
		while (true) {
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setShardIterator(getShards(streamName, shard));
			getRecordsRequest.setLimit(25);

			GetRecordsResult getRecordsResult = getKin().getRecords(
					getRecordsRequest);
			records = getRecordsResult.getRecords();

			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				throw new RuntimeException(exception);
			}

			shardIterator = getRecordsResult.getNextShardIterator();
			
			if(shardIterator==null) break;
		}
		return records;
	}
	
	public List<Shard> getAllShards(String streamName){
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName( streamName );
		List<Shard> shards = new ArrayList<Shard>();
		String exclusiveStartShardId = null;
		do {
		    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
		    DescribeStreamResult describeStreamResult = getKin().describeStream( describeStreamRequest );
		    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
		    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
		        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
		    } else {
		        exclusiveStartShardId = null;
		    }
		} while ( exclusiveStartShardId != null );
		return shards;
	}
	
	public void deleteStream(String streamName){
		// Delete the stream.
        LOG.info("Deleting stream : " + streamName);
		DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
		deleteStreamRequest.setStreamName(streamName);
		getKin().deleteStream(deleteStreamRequest);
		// The stream is now being deleted.
        LOG.info("Stream is now being deleted : " + streamName);
	}
}

