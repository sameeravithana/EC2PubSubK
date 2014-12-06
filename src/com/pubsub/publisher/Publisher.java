package com.pubsub.publisher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Publisher {
	private static final Log LOG = LogFactory.getLog(Publisher.class);
	
	private PublicationFactory pubFactory;
	private AmazonKinesis kinesis;
	private String streamName;
	
	private final ObjectMapper JSON = new ObjectMapper();

	public Publisher(PublicationFactory pubFactory, AmazonKinesis kinesis, String streamName){
		    if (pubFactory == null) {
	            throw new IllegalArgumentException("pubFactory must not be null");
	        }
	        if (kinesis == null) {
	            throw new IllegalArgumentException("kinesis must not be null");
	        }
	        if (streamName == null || streamName.isEmpty()) {
	            throw new IllegalArgumentException("streamName must not be null or empty");
	        }
	        this.pubFactory = pubFactory;
	        this.kinesis = kinesis;
	        this.streamName = streamName;
		
	}
	
	/**
     * Send a fixed number of Publications to Amazon Kinesis. This sends them sequentially.
     * If you require more throughput consider using multiple {@link Publication}s.
     *
     * @param n The number of publications to send to Amazon Kinesis.
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @throws InterruptedException Interrupted while waiting to send the next pair.
     */
    public void sendPairs(long n, long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        for (int i = 0; i < n && !Thread.currentThread().isInterrupted(); i++) {
        	sendPublication();
        	if (delayBetweenRecords > 0) {
        		Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
        	}
        }
    }

    /**
     * Continuously sends Publications to Amazon Kinesis sequentially. This will only stop if interrupted. If you
     * require more throughput consider using multiple {@link HttpReferrerKinesisPutter}s.
     *
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @throws InterruptedException Interrupted while waiting to send the next pair.
     */
    public void sendPairsIndefinitely(long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            sendPublication();
            if (delayBetweenRecords > 0) {
                Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
            }
        }
    }
    
    public void sendWindowAll(long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        List<Publication> pubList=pubFactory.getPublications();
    	for (int i = 0; i < pubFactory.getFactorySize() && !Thread.currentThread().isInterrupted(); i++) {
        	sendWindowPublication(pubList.get(i));
        	if (delayBetweenRecords > 0) {
        		Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
        	}
        }
    }
    
    /**
     * Send a single pair to Amazon Kinesis using PutRecord.
     */
    private void sendPublication() {
        Publication publication = pubFactory.create();
        byte[] bytes;
        try {
            bytes = JSON.writeValueAsBytes(publication);
        } catch (IOException e) {
            LOG.warn("Skipping pair. Unable to serialize: '" + publication + "'", e);
            return;
        }

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        // We use the resource as the partition key so we can accurately calculate totals for a given resource
        putRecord.setPartitionKey(publication.getPclass());
        putRecord.setData(ByteBuffer.wrap(bytes));
        // Order is not important for this application so we do not send a SequenceNumberForOrdering
        putRecord.setSequenceNumberForOrdering(null);
        
        

        try {
            kinesis.putRecord(putRecord);
            System.out.println("Push Publication: "+publication.getPclass()+" "+publication.getData()+" "+publication.getIssuedTime());
        } catch (ProvisionedThroughputExceededException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Thread %s's Throughput exceeded. Waiting 10ms", Thread.currentThread().getName()));
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }

    /**
     * Send a single pair to Amazon Kinesis using PutRecord.
     */
    private void sendWindowPublication(Publication publication) {
        //Publication publication = pubFactory.create();
        byte[] bytes;
        try {
            bytes = JSON.writeValueAsBytes(publication);
        } catch (IOException e) {
            LOG.warn("Skipping pair. Unable to serialize: '" + publication + "'", e);
            return;
        }

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        // We use the resource as the partition key so we can accurately calculate totals for a given resource
        putRecord.setPartitionKey(publication.getPclass());
        putRecord.setData(ByteBuffer.wrap(bytes));
        // Order is not important for this application so we do not send a SequenceNumberForOrdering
        putRecord.setSequenceNumberForOrdering(null);
        
        

        try {
            kinesis.putRecord(putRecord);
            System.out.println("Push Publication: "+publication.getPclass()+" "+publication.getData()+" "+publication.getIssuedTime());
        } catch (ProvisionedThroughputExceededException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Thread %s's Throughput exceeded. Waiting 10ms", Thread.currentThread().getName()));
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
