/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.compute.kinesis.slidingwindow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.pubsub.publisher.Publication;
import com.pubsub.subindex.InvertedIndex;

/**
 * Provides a way to count the occurrences of objects across a number of discrete "buckets". These buckets usually
 * represent a time period such as 1 second.
 */
public class BucketBasedCounter<ObjectType> {
    private Map<Publication, Double> objectCounts;
    
    private Map<Integer,List<Publication>> bucketObjects;
    private int maxBuckets;
    private InvertedIndex idx;
    
    

    /**
     * Create a new counter with a fixed number of buckets.
     * 
     * @param maxBuckets Total buckets this counter will use.
     * @throws IOException 
     */
    public BucketBasedCounter(int maxBuckets) throws IOException {
        if (maxBuckets < 1) {
            throw new IllegalArgumentException("maxBuckets must be >= 1");
        }
        objectCounts = new HashMap<Publication, Double>();
        bucketObjects=new HashMap<Integer,List<Publication>>();
        
        this.maxBuckets = maxBuckets;
        
       
    }
    
    public BucketBasedCounter(int maxBuckets, InvertedIndex idx) throws IOException {
        if (maxBuckets < 1) {
            throw new IllegalArgumentException("maxBuckets must be >= 1");
        }
        objectCounts = new HashMap<Publication, Double>();
        bucketObjects=new HashMap<Integer,List<Publication>>();
        
        this.maxBuckets = maxBuckets;
        this.idx=idx;
       
    }

    /**
     * Increment the count of the object for a specific bucket index.
     * 
     * @param obj Object whose count should be updated.
     * @param bucket Index of bucket to increment.
     * @return The new count for that object at the bucket index provided.
     */
    public double increment(ObjectType obj, int headBucket) {  
    	double decayRelScore=0;
    	Publication pobj=(Publication)obj;
    	
    	if(bucketObjects.get(headBucket)==null){
    		List<Publication> objs=new LinkedList<Publication>();
    		bucketObjects.put(headBucket, objs);
    	}
    	
    	   	
    	
    	//decayRelScore=idx.matchPublication(pobj);
    	
    	//Random rand=new Random(500);
    	//decayRelScore=rand.nextDouble();
    	decayRelScore=idx.matchPublication(pobj);
    	
    	if(decayRelScore>0){
    		System.out.println("MATCHING Publication: "+pobj.getPclass()+" Relevancy score: "+decayRelScore+"\n+++++++++++++\n");
    		pobj.setDecayRelScore(decayRelScore);
    		bucketObjects.get(headBucket).add(pobj); 
    		objectCounts.put(pobj, decayRelScore);
    	}
        
        return decayRelScore;
    }

    /**
     * Computes the total count for all objects across all buckets.
     * 
     * @return A mapping of object to total count across all buckets.
     */
    public Map<Integer,List<Publication>> getCounts() {       
    	return bucketObjects;
    }

    /**
     * Calculates the sum total of occurrences across all bucket counts.
     * 
     * @param counts A set of count buckets from the same object.
     * @return The sum of all counts in the buckets.
     */
    private long calculateTotal(long[] counts) {
        long total = 0;
        for (long count : counts) {
            total += count;
        }
        return total;
    }

    /**
     * Remove any objects whose buckets total 0.
     */
    public void pruneEmptyObjects() {
        List<Publication> toBePruned = new ArrayList<Publication>();
        for (Map.Entry<Publication, Double> entry : objectCounts.entrySet()) {
            // Remove objects whose total counts are 0
            if (entry.getValue() == 0) {
                toBePruned.add(entry.getKey());
            }
        }
        for (Publication prune : toBePruned) {
            objectCounts.remove(prune);
        }
        
       
        
    }

    /**
     * Clears all object counts for the given bucket. If you wish to remove objects that no longer have any counts in
     * any bucket use {@link #pruneEmptyObjects()}.
     * 
     * @param bucket The index of the bucket to clear.
     */
    public void clearBucket(int bucket) {
    	if(bucketObjects.get(bucket)!=null)
    		bucketObjects.get(bucket).clear();
    }
}
