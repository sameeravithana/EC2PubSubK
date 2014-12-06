package com.pubsub.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

public class Utility {
	
	private File file;
	private int size;	
	private LinkedList<String> atts;
	
	public Utility(File file) throws IOException{		
		this.file = file;	
		this.atts = readFile();		
		this.size = this.atts.size();
		
	}
	
	public LinkedList<String> readFile() throws IOException{
		FileReader fr = new FileReader(file.getAbsoluteFile());
		
		BufferedReader br = new BufferedReader(fr);		
	
		String line;
		LinkedList<String> atts=new LinkedList<String>();
		while((line=br.readLine())!=null){			
				String[] tuple = line.split("=", 2);
				String att = tuple[0];
				att.replaceAll("\\s","");
				atts.add(att);
		}
		br.close();
		return atts;
	}
	
	public String[] getAttributesArray(){	
		return this.atts.toArray(new String[this.size]);
	}
	
	public void writeFile(String wpage, String outputFileName, boolean append) throws IOException, InterruptedException{
		String cfolderName=this.file.getName().split("_")[2];
		String folderPath="gen//ZipfAttributes//LocalZipf//"+cfolderName;
		
		File cfolder = new File(folderPath);	
		
		if (!cfolder.exists()) {
			cfolder.mkdir();
		}
		
		String filePath=folderPath+"//"+outputFileName;
		//String filePath="gen//ZipfAttributes//LocalZipf//"+outputFileName;
		File afile = new File(filePath);					
		
		if (!afile.exists()) {
			afile.createNewFile();
		}
		
		FileWriter afw = new FileWriter(afile.getAbsoluteFile(),append);
		BufferedWriter abw = new BufferedWriter(afw);
		
		abw.write(wpage);
		
		Thread.sleep(2000);
		
		abw.close();
	}

	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public File getFile() {
		return file;
	}

	public LinkedList<String> getAtts() {
		return atts;
	}

}
