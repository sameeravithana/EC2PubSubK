package com.amazonaws.compute.kinesis.relevancy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.compute.kinesis.KinesisDriver;
import com.amazonaws.compute.kinesis.counter.CountingRecordProcessorConfig;
import com.amazonaws.compute.kinesis.persist.CountPersister;
import com.amazonaws.compute.kinesis.slidingwindow.SlidingWindowCounter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.operation.utils.Clock;
import com.operation.utils.NanoClock;
import com.operation.utils.Timer;
import com.pubsub.publisher.Publication;
import com.pubsub.subindex.GenerateIndex;
import com.pubsub.subindex.InvertedIndex;

public class RelevancyRecordProcessor<T> implements IRecordProcessor{
	private static final Log LOG = LogFactory.getLog(RelevancyRecordProcessor.class);

    // Lock to use for our timer
    private static final Clock NANO_CLOCK = new NanoClock();
    // The timer to schedule checkpoints with
    private Timer checkpointTimer = new Timer(NANO_CLOCK);

    // Our JSON object mapper for deserializing records
    private final ObjectMapper JSON;

    // Interval to calculate distinct counts across
    private int computeIntervalInMillis;
    // Total range to consider counts when calculating totals
    private int computeRangeInMillis;

    // Counter for keeping track of counts per interval.
    private SlidingWindowCounter<T> counter;

    // The shard this processor is processing
    private String kinesisShardId;

    // We schedule count updates at a fixed rate (computeIntervalInMillis) on a separate thread
    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);

    // This is responsible for persisting our counts every interval
    //private CountPersister<T> persister;
    
    private KinesisDriver kdriver;

    private CountingRecordProcessorConfig config;

    // The type of record we expect to receive as JSON
    private Class<T> recordType;
    
	private InvertedIndex idx;


	public RelevancyRecordProcessor(CountingRecordProcessorConfig config,
			Class<T> recordType,
			InvertedIndex idx,
			int computeRangeInMillis, int computeIntervalInMillis) throws IOException {
    	if (config == null) {
            throw new NullPointerException("config must not be null");
        }
        if (recordType == null) {
            throw new NullPointerException("recordType must not be null");
        }
        if (idx == null) {
            throw new NullPointerException("subscription index must not be null");
        }
        if (computeRangeInMillis <= 0) {
            throw new IllegalArgumentException("computeRangeInMillis must be > 0");
        }
        if (computeIntervalInMillis <= 0) {
            throw new IllegalArgumentException("computeIntervalInMillis must be > 0");
        }
        if (computeRangeInMillis % computeIntervalInMillis != 0) {
            throw new IllegalArgumentException("compute range must be evenly divisible by compute interval to support "
                    + "accurate intervals");
        }
        
        this.config = config;
        this.recordType = recordType;
        this.kdriver = new KinesisDriver("A1Stream", 2);
        this.computeRangeInMillis = computeRangeInMillis;
        this.computeIntervalInMillis = computeIntervalInMillis;
        this.idx=idx;

        // Create an object mapper to deserialize records that ignores unknown properties
        JSON = new ObjectMapper();
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);        		
        
        System.out.println("RelevancyRecordProcessor");
	}
	
	@Override
	public void initialize(String shardId) {
		kinesisShardId = shardId;
        resetCheckpointAlarm();

        System.out.println("Shard Id: "+shardId);
        //System.out.println("Init: DynamoDB Persister ");
        //persister.initialize();

        System.out.println("Init: Sliding Window");
        // Create a sliding window whose size is large enough to hold an entire range of individual interval counts.
        try {
			counter = new SlidingWindowCounter<T>((int) (computeRangeInMillis / computeIntervalInMillis), idx);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Create a scheduled task that runs every computeIntervalInMillis to compute and
        // persist the counts.
        scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Synchronize on the counter so we stop advancing the interval while we're checkpointing
                synchronized (counter) {
                    try {
                    	//System.out.println("AdvanceOneInterval");
                        advanceOneInterval();
                    } catch (Exception ex) {
                        LOG.warn("Error advancing sliding window one interval (" + computeIntervalInMillis
                                + "ms). Skipping this interval.", ex);
                    }
                }
            }
        },
                TimeUnit.SECONDS.toMillis(config.getInitialWindowAdvanceDelayInSeconds()),
                computeIntervalInMillis,
                TimeUnit.MILLISECONDS);
	}
	
	/**
     * Advance the internal sliding window counter one interval. This will invoke our count persister if the window is
     * full.
	 * @throws InterruptedException 
	 * @throws IOException 
     */
    protected void advanceOneInterval() throws IOException, InterruptedException {
        Map<Integer,List<Publication>> counts = null;
        synchronized (counter) {
            // Only persist the counts if we have a full range of data to report. We don't want partial
            // counts each time the process starts.
            if (shouldPersistCounts()) {
                counts = counter.getCounts();
                //counter.pruneEmptyObjects();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("We have not collected enough interval samples to calculate across the "
                            + "entire range from shard %s. Skipping this interval.", kinesisShardId));
                }
            }
            //System.out.println("AdvanceOneInterval_Stepin");
            counter.advanceWindow();
            
        }
        // Persist the counts if we have a full range
        if (counts != null) {
        	kdriver.streamWritePub(counts);
        }
    }
    
    private boolean shouldPersistCounts() {
        return counter.isWindowFull();
    }

	@Override
	public void processRecords(List<Record> records,
			IRecordProcessorCheckpointer checkpointer) {
		System.out.println("Matching...");
    	for (Record r : records) {
            // Deserialize each record as an UTF-8 encoded JSON String of the type provided
            T pair;
            try {
                pair = JSON.readValue(r.getData().array(), recordType);
            } catch (IOException e) {
                LOG.warn("Skipping record. Unable to parse record into Publication. Partition Key: "
                        + r.getPartitionKey() + ". Sequence Number: " + r.getSequenceNumber(),
                        e);
                continue;
            }
            // Increment the counter for the new pair. This is synchronized because there is another thread reading from
            // the counter to compute running totals every interval.
            
            synchronized (counter) {
            	System.out.println("Matching: "+r.getSequenceNumber());
                counter.increment(pair);
            }
            System.out.println("\n");
        }

        // Checkpoint if it's time to!
        if (checkpointTimer.isTimeUp()) {
            // Obtain a lock on the counter to prevent additional counts from being calculated while checkpointing.
            synchronized (counter) {
                checkpoint(checkpointer);
                resetCheckpointAlarm();
            }
        }
		
	}

	@Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);

        scheduledExecutor.shutdown();
        try {
            // Wait for at most 30 seconds for the executor service's tasks to complete
            if (!scheduledExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Failed to properly shut down interval thread pool for calculating interval counts and persisting them. Some counts may not have been persisted.");
            } else {
                // Only checkpoint if we successfully shut down the thread pool
                // Important to checkpoint after reaching end of shard, so we can start processing data from child
                // shards.
                if (reason == ShutdownReason.TERMINATE) {
                    synchronized (counter) {
                        checkpoint(checkpointer);
                    }
                }
            }
        } catch (InterruptedException ie) {
            // We failed to shutdown cleanly, do not checkpoint.
            scheduledExecutor.shutdownNow();
            // Handle this similar to a host or process crashing and abort the JVM.
            LOG.fatal("Couldn't successfully persist data within the max wait time. Aborting the JVM to mimic a crash.");
            System.exit(1);
        }
    }

    /**
     * Set the timer for the next checkpoint.
     */
    private void resetCheckpointAlarm() {
        checkpointTimer.alarmIn(config.getCheckpointIntervalInSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < config.getCheckpointRetries(); i++) {
            try {
                // First checkpoint our persister to guarantee all calculated counts have been persisted
                //persister.checkpoint();
                checkpointer.checkpoint();
                return;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                return;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (config.getCheckpointRetries() - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + config.getCheckpointRetries(),
                            e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(config.getCheckpointBackoffTimeInSeconds());
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
        // Handle this similar to a host or process crashing and abort the JVM.
        LOG.fatal("Couldn't successfully persist data within max retry limit. Aborting the JVM to mimic a crash.");
        System.exit(1);
    }

}
