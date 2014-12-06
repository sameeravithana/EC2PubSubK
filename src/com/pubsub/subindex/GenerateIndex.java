package com.pubsub.subindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class GenerateIndex {
	
	String path = "gen/OriginalSubscriptions";
	ClassLoader classLoader = getClass().getClassLoader();
	File folder;	
	
	InvertedIndex idx;
	
	public GenerateIndex(){
		this.idx=new InvertedIndex();		
		
		folder=new File(classLoader.getResource(path).getFile());
		
		System.out.println("Subscription Folder: "+folder.getAbsolutePath());
		
	}
	
	public void generate() throws IOException{
		
		for (File subfile : folder.listFiles()) {

			System.out.println("Generating index: " + subfile.getName());

			idx.indexFile(subfile);

			System.out.println("AVG WEIGHT: " + idx.getAvgWeight() + "\n");
			
			break;
		}
	}
	
	public void generate(BufferedReader br) throws IOException{	
			idx.indexFile(br);

			System.out.println("AVG WEIGHT: " + idx.getAvgWeight() + "\n");			
	}

	public InvertedIndex getIdx() {
		return idx;
	}
	

}
