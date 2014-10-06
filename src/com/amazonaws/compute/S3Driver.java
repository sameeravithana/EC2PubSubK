package com.amazonaws.compute;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Driver {
	private static String bucketName = "elasticbeanstalk-us-east-1-511368698353"; 
	private static String key        = "*** provide object key ***"; 

	AmazonS3Client s3;
	public AmazonS3Client getS3() {
		return s3;
	}
		
	public S3Driver(){
		s3=new ProfileCredentials().getS3Client();
		//getObjectKeys();
	}
	
	public List<String> getObjectKeys(){
		List<String> okeys=new LinkedList<String>();
		// Send sample request (list objects in a given bucket).
		ObjectListing objectListing = s3.listObjects(new 
		     ListObjectsRequest().withBucketName(bucketName));
		for(S3ObjectSummary s3ob:objectListing.getObjectSummaries()){
			String okey=s3ob.getKey();
			if(okey.startsWith("resources/datasets/")){
				okeys.add(okey);
			}
			//System.out.println(s3ob.getBucketName()+" "+s3ob.getKey());
			
		}
		return okeys;
	}
	
	public InputStream getObjectInput(String key){
		S3Object s3object = s3.getObject(new GetObjectRequest(
        		bucketName, key));
        System.out.println("Read DataFile | Content-Type: "  + 
        		s3object.getObjectMetadata().getContentType());
        return s3object.getObjectContent();
	}

	
}
