package com.amazonaws.compute.kinesis.relevancy;

import java.io.IOException;

import com.amazonaws.compute.kinesis.KinesisDriver;
import com.amazonaws.compute.kinesis.counter.CountingRecordProcessor;
import com.amazonaws.compute.kinesis.counter.CountingRecordProcessorConfig;
import com.amazonaws.compute.kinesis.persist.CountPersister;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.pubsub.subindex.InvertedIndex;


/**
 * Generates {@link CountingRecordProcessor}s for counting occurrences of unique values over a given range.
 *
 * @param <T> The type of records the processors this factory creates are capable of counting.
 */
public class RelevancyRecordProcessorFactory<T> implements IRecordProcessorFactory {

    private Class<T> recordType;
    //private CountPersister<T> persister;
    private InvertedIndex idx;
    private int computeRangeInMillis;
    private int computeIntervalInMillis;
    private CountingRecordProcessorConfig config;

    /**
     * Creates a new factory that uses the default configuration values for each
     * processor it creates.
     *
     * @see #CountingRecordProcessorFactory(Class, CountPersister, int, int, CountingRecordProcessorConfig)
     */
    public RelevancyRecordProcessorFactory(Class<T> recordType,
            KinesisDriver kdriver,
            int computeRangeInMillis,
            int computeIntervalInMillis) {
        //this(recordType, computeRangeInMillis, computeIntervalInMillis, new CountingRecordProcessorConfig());
    	//this()
    }

    /**
     * Create a new factory that produces counting record processors that sum counts over a range and update those
     * counts at each interval.
     *
     * @param recordType The type of records the processors this factory creates are capable of counting.
     * @param persister Persister to use for storing the counts.
     * @param computeRangeInMillis Range, in milliseconds, to compute the count across.
     * @param computeIntervalInMillis Milliseconds between count updates. This is the frequency at which the persister
     *        will be called.
     * @param config The configuration to use for each created counting record processor.
     *
     * @throws IllegalArgumentException if computeRangeInMillis or computeIntervalInMillis are not greater than 0 or
     *         computeRangeInMillis is not evenly divisible by computeIntervalInMillis.
     */
    public RelevancyRecordProcessorFactory(Class<T> recordType, 
    		InvertedIndex idx,
            int computeRangeInMillis,
            int computeIntervalInMillis) {
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

        this.recordType = recordType;
        this.idx=idx;
        this.computeRangeInMillis = computeRangeInMillis;
        this.computeIntervalInMillis = computeIntervalInMillis;
        this.config = new CountingRecordProcessorConfig();
    }

    /**
     * Creates a counting record processor that sums counts over the provided compute range and updates those counts
     * every interval.
     */    
	@Override
    public IRecordProcessor createProcessor(){        
		RelevancyRecordProcessor<T> rrp=null;
		try {
			rrp=new RelevancyRecordProcessor<T>(config,
			        recordType,
			        idx,
			        computeRangeInMillis,
			        computeIntervalInMillis);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rrp;
		
    }
}
