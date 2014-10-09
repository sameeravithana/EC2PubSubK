package com.amazonaws.compute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.compute.kinesis.RecordProcessorFactory;
import com.amazonaws.compute.kinesis.counter.CountingRecordProcessorFactory;
import com.amazonaws.compute.kinesis.counter.DynamoDBCountPersister;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
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
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.pubsub.model.Publication;
import com.pubsub.model.PublicationFactory;
import com.pubsub.publisher.Publisher;

public class KinesisDriver {
	
	AmazonKinesisClient kin;
	
	S3Driver s3_drv;
	
	DynamoDBDriver ddb_drv;
	
	private KinesisClientLibConfiguration kinesisClientLibConfiguration;
	
	private static String applicationName = "EC2PubSubV101";
	private static String streamName = "EC2Stream2";
	private static String kinesisEndpoint = "http://kinesis.us-east-1.amazonaws.com";
	
	
	/**
	 * ATSEQUENCENUMBER to start at given sequence number.
	 * AFTERSEQUENCENUMBER to start after a given sequence number.
	 * TRIM_HORIZON to start with the oldest stored record.
	 * LATEST to start with new records as they arrive
	 */
	private static InitialPositionInStream initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
	
	private static ShardIteratorType readShardIType = ShardIteratorType.TRIM_HORIZON;
	
	private static final Log LOG = LogFactory.getLog(KinesisDriver.class);
	
	/**
     * The amount of time to wait between records.
     *
     * We want to send at most 10 records per second per thread so we'll delay 100ms between records.
     * This keeps the overall cost low for this sample.
     */
    private static final long DELAY_BETWEEN_RECORDS_IN_MILLIS = 100;
    
    /**
     * Number of threads writing data to the stream
     * records per second = (1000 / DELAY_BETWEEN_RECORDS_IN_MILLIS) * numberOfThreads; 10*5=50
     */
    private int numberOfThreads = 5;
    
 // Count occurrences of HTTP referrer pairs over a range of 10 seconds
    private static final int COMPUTE_RANGE_FOR_COUNTS_IN_MILLIS = 10000;
    // Update the counts every 1 second
    private static final int COMPUTE_INTERVAL_IN_MILLIS = 1000;
	
	AWSCredentialsProvider credentialsProvider=null;

	/**
	 * in <- records per second
	 * out -> readCount
	 * Configuration in = out
	 */
	private int inRecordsPerSecond = (int) ((1000 / DELAY_BETWEEN_RECORDS_IN_MILLIS) * numberOfThreads);
	private int outReadCount = inRecordsPerSecond;

	/**
	 * Number of seconds that page is going to refresh
	 */
	private static int interval = 1;
	
	public String countsTableName = "EC2PubSubCount";

	

	public AmazonKinesisClient getKin() {
		return kin;
	}

	public KinesisDriver(){
		credentialsProvider=new ProfileCredentials();
		
		kin=((ProfileCredentials) credentialsProvider).getKinesisClient();
		
		s3_drv=new S3Driver();
			//kin.setEndpoint("us-east-1", "kinesis", "http://kinesis.us-east-1.amazonaws.com");
		
		ddb_drv=new DynamoDBDriver();
		
	}	
	
	/**
	 * Create a Kinesis Stream
	 * @param streamName
	 * @param streamSize
	 */
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
		
	/**
	 * List all available Kinesis Streams
	 * @return
	 */
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
	
	/**
	 * Adding Data records to the stream by an external source file
	 * @param streamName
	 * @param _dataurl
	 * @return
	 * @throws IOException
	 */
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
		
		lastSequenceNumber=putRecordResult.getSequenceNumber();
		return lastSequenceNumber;
	}
	
	/**
	 * Push a generated data stream
	 * @throws InterruptedException
	 */
	public void streamWrite() throws InterruptedException{
		List<String> resources = new ArrayList<String>();
		resources.add("SamTube405");
		
		List<String> referees = new ArrayList<String>();
		referees.add("iPhone6");
		referees.add("Nexus5");
		referees.add("OnePlusOne");
		referees.add("G2");
		referees.add("NeoL");
		referees.add("Moto7");
		
		PublicationFactory pubFactory=new PublicationFactory(resources,referees);
		
		final Publisher publisher=new Publisher(pubFactory, getKin(), streamName);
		
		ExecutorService es = Executors.newCachedThreadPool();

        Runnable pairSender = new Runnable() {
            @Override
            public void run() {
                try {
                	publisher.sendPairsIndefinitely(DELAY_BETWEEN_RECORDS_IN_MILLIS, TimeUnit.MILLISECONDS);
                	//publisher.sendPairs(20,DELAY_BETWEEN_RECORDS_IN_MILLIS, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    LOG.warn("Thread encountered an error while sending records. Records will no longer be put by this thread.",
                            ex);
                }
            }
        };

        for (int i = 0; i < numberOfThreads ; i++) {
            es.submit(pairSender);
        }

        System.out.println(String.format("Sending publication with a %dms delay between records with %d thread(s).",
                DELAY_BETWEEN_RECORDS_IN_MILLIS,
                numberOfThreads));

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);        
       
	}
	
	/**
	 * Add your Logic inside a stream
	 * @throws UnknownHostException
	 */
	public void startProcessingData() throws UnknownHostException{	
		boolean flag=false;
        System.out.println("Processing stream " + streamName);
        
        
        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        System.out.println("Using workerId: " + workerId);
        
        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream);
        
        IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
        
        int exitCode = 0;
        try {
        	System.out.println("Worker Thread Running.");
            worker.run();
            
            flag=true;
        } catch (Throwable t) {
            System.out.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);        
	}
	
	/**
	 * Start counter application on the stream
	 * @throws UnknownHostException
	 */
	public void startCounterApp() throws UnknownHostException{
		System.out.println("Starting Counter App on " + streamName);
		ddb_drv.createCountTableIfNotExists(countsTableName);
		System.out.println(String.format("%s DynamoDB table is ready for use", countsTableName));
		

        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");
        
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        System.out.println("Using workerId: " + workerId);
        
        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream);
        
        // Persist counts to DynamoDB
        DynamoDBCountPersister persister =
                new DynamoDBCountPersister(ddb_drv.createMapperForTable(countsTableName));
        
        IRecordProcessorFactory recordProcessor =
                new CountingRecordProcessorFactory<Publication>(Publication.class,
                        persister,
                        COMPUTE_RANGE_FOR_COUNTS_IN_MILLIS,
                        COMPUTE_INTERVAL_IN_MILLIS);

        Worker worker = new Worker(recordProcessor, kinesisClientLibConfiguration);

        System.out.println("Starting workers.");
        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
	}
	
	/**
	 * Get available data records
	 * @return
	 */
	public List<Record> getDataRecords(){
		System.out.println("Getting Shard Details..");
		List<Shard> shards=getAllShards(streamName);
		
		List<Record> records=new ArrayList<Record>();
		for(Shard shard:shards){
			System.out.println("Shard: "+shard.getShardId());
			List<Record> srecords=getShardRecords(streamName, shard);
			System.out.println("   Getting shard records");
			for(Record record: srecords){
				String d = new String(record.getData().array(),	Charset.forName("UTF-8"));
				System.out.println("    Record: "+d);
				records.add(record);
			}
			
		}
		return records;
	}
	
	public String getShards(String streamName, Shard shard){
		String shardIterator;
		  GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		  getShardIteratorRequest.setStreamName(streamName);
		  getShardIteratorRequest.setShardId(shard.getShardId());		  
		  getShardIteratorRequest.setShardIteratorType(readShardIType);
		  

		  GetShardIteratorResult getShardIteratorResult = getKin().getShardIterator(getShardIteratorRequest);
		  shardIterator = getShardIteratorResult.getShardIterator(); 
		  return shardIterator;
	}
	
	public List<Record> getShardRecords(String streamName, Shard shard) {
		// Continuously read data records from a shard
		List<Record> records = new ArrayList<Record>();
		String shardIterator=getShards(streamName, shard);
		while (true) {
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();			
			getRecordsRequest.setShardIterator(shardIterator);
			getRecordsRequest.setLimit(inRecordsPerSecond);

			GetRecordsResult getRecordsResult = getKin().getRecords(
					getRecordsRequest);
			records.addAll(getRecordsResult.getRecords());			
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				throw new RuntimeException(exception);
			}

			shardIterator = getRecordsResult.getNextShardIterator();
			
			if(shardIterator==null) break;
			
			System.out.println("    Retrieve Records size: "+records.size()+" Shard Iterator: "+shardIterator);
			
			if(records.size()> outReadCount*interval ) break;
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
	
	/**
	 * Delete the given stream
	 * @param streamName
	 */
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

