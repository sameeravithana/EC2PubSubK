package com.amazonaws.compute;

import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.ProfileCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;


public class EC2Driver {
	
	static SNSDriver ec2Driver;
	
	AWSCredentials credentials;
	public AWSCredentials getCredentials() {
		return credentials;
	}

	AmazonEC2Client ec2;
	public AmazonEC2Client getEc2() {
		return ec2;
	}

	Region region;
	
	public EC2Driver(){		
		ec2 = new ProfileCredentials().getEC2Client();
        region = Region.getRegion(Regions.US_EAST_1);
        ec2.setRegion(region);
	}

	public List<String> getEC2Instances(){
		DescribeInstancesRequest request = new DescribeInstancesRequest();
       
        DescribeInstancesResult result = ec2.describeInstances(request);

        List<Reservation> reservations = result.getReservations();
        
        List<String> instanceList = new LinkedList<String>();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();

            for (Instance instance : instances) {
            	String insId=instance.getInstanceId();
            	instanceList.add(insId);
                System.out.println("	Instance: "+insId);


            }
        } 
        
        return instanceList;
	}
}
