package com.amazonaws.compute;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;

public class KinesisDriver {
	
	AmazonKinesisClient kin;

	public AmazonKinesisClient getKin() {
		return kin;
	}

	public KinesisDriver(){
		kin=new ProfileCredentials().getKinesisClient();
			//kin.setEndpoint("us-east-1", "kinesis", "http://kinesis.us-east-1.amazonaws.com");
	}
	
	public void createStream(String streamName, int streamSize) {
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(streamName);
		createStreamRequest.setShardCount(streamSize);

		getKin().createStream(createStreamRequest);
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(20 * 1000);
			} catch (Exception e) {
			}

			try {
				DescribeStreamResult describeStreamResponse = getKin().describeStream(describeStreamRequest);
				String streamStatus = describeStreamResponse
						.getStreamDescription().getStreamStatus();
				if (streamStatus.equals("ACTIVE")) {
					break;
				}
				//
				// sleep for one second
				//
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			} catch (ResourceNotFoundException e) {
			}
		}
		if (System.currentTimeMillis() >= endTime) {
			throw new RuntimeException("Stream " + streamName
					+ " never went active");
		}
	}
	
	public void deleteStream(String streamName){
		DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
		deleteStreamRequest.setStreamName(streamName);
		getKin().deleteStream(deleteStreamRequest);
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
	
	public String addDataRecords(String streamName){
		String sequenceNumberOfPreviousRecord= null;
		String lastSequenceNumber =null;
		PutRecordResult putRecordResult=null;
		for (int j = 0; j < 10; j++) 
		{
		  PutRecordRequest putRecordRequest = new PutRecordRequest();
			  putRecordRequest.setStreamName(streamName);
			  putRecordRequest.setData(ByteBuffer.wrap( String.format( "testData-%d", j ).getBytes() ));
			  putRecordRequest.setPartitionKey( String.format( "partitionKey-%d", j%2 ));			  
			  putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
			  
		  putRecordResult = getKin().putRecord( putRecordRequest );
		  sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
		}
		
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
}

