package com.pubsub.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;

public class SubUtility {
	
	private File subfile;
	Map<String, LinkedHashSet<String>> attsvalues;
	
	public SubUtility(File subfile) throws IOException{
		this.subfile=subfile;
		attsvalues=new HashMap<String, LinkedHashSet<String>>();
		
		readFile();
		
	}

	public void readFile() throws IOException{
		FileReader fr = new FileReader(this.subfile.getAbsoluteFile());
		
		BufferedReader br = new BufferedReader(fr);		
	
		String line;
		LinkedList<String> atts=new LinkedList<String>();
		
		String att="";
		LinkedHashSet<String> avalues=null;
		
		while((line=br.readLine())!=null){	
			if(line.startsWith("ATTRIBUTE:")){
				att=line.split(":")[1];
				att=att.replaceAll("\\s","");	
				avalues=new LinkedHashSet<String>();
			}else if(!line.startsWith("VALUES:") & !line.equals("")){
				avalues.add(line);
			}
			
			if(line.isEmpty()){				
				attsvalues.put(att, avalues);
				System.out.println("Attribute: "+att+" Added Records: "+avalues.size());
				//System.out.println(attsvalues.toString());
			}				
		}
		br.close();
		
	}

	public Map<String, LinkedHashSet<String>> getAttsvalues() {
		return attsvalues;
	}

	
	
}
