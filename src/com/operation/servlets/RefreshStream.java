package com.operation.servlets;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.compute.Engine;
import com.amazonaws.compute.kinesis.KinesisDriver;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.publisher.Publication;


/**
 * Servlet implementation class RefreshStream
 */
@WebServlet("/refreshS")
public class RefreshStream extends HttpServlet {
	private static final long serialVersionUID = 1L;
	GsonWriter gsonwrt;
	
	private final static ObjectMapper JSON = new ObjectMapper();
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public RefreshStream() {
        super();
        gsonwrt=new GsonWriter();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Map<String,Object> map=new HashMap<String,Object>();
		boolean isValid=false;
		System.out.println("Action Binded: Refreshing Stream");
		List<Record> records=new KinesisDriver("AStream", 2).getDataRecords();
		
		for(Record record:records) { 
			HashMap<String, Object> rmap=new HashMap<String,Object>();
			String dataRecord = new String(record.getData().array(),Charset.forName("UTF-8")); 
			Publication pubRecord = JSON.readValue(dataRecord, Publication.class);				
			String partitionKey = record.getPartitionKey(); 
			String seqNumber = record.getSequenceNumber(); 
				rmap.put("publication", dataRecord);
				rmap.put("partitionKey",partitionKey);
				rmap.put("seqNumber",seqNumber);
				
				map.put(seqNumber, rmap);
				
				System.out.println("Publication: "+dataRecord+ " Partitionkey: "+partitionKey+" SeqNo: "+seqNumber);
		}
		
		isValid=true;
		
		map.put("isValid", isValid);
		gsonwrt.write(response,map);
	}
	
	
		

}
