package com.pubsub.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

class SubThread extends Thread {
	private Thread t;
	Random rand=new Random();
	Utility utility;
	SubUtility subutility;
	private LinkedList<String> attributes;
	private Map<String, LinkedHashSet<String>> attributesValues;
	String[] ops={"po","so","eq"};
	
	File afile;
	File sfile;
	File zfolder;
	String _endPoint;
	

	public SubThread(File afile,File sfile, File zfolder,String _endPoint) {
		this.afile=afile;
		this.sfile=sfile;	
		this.zfolder=zfolder;
		this._endPoint=_endPoint;
	}
	
	public SubThread(File afile, File sfile, File zfolder) {
		this.afile=afile;
		this.sfile=sfile;	
		this.zfolder=zfolder;		
	}

	public SubThread(File sfile) {
		this.sfile=sfile;
	}

	public void run() {
		
		/*try {
			utility = new Utility(afile);
			subutility = new SubUtility(sfile);

			this.attributes = utility.getAtts();
			this.attributesValues = subutility.getAttsvalues();

			System.out.println("Starting genSub();");
			genSubs();
			
			//print();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		try {
			genRightSubs();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void genRightSubs() throws IOException, InterruptedException{
		String wpage="";
		
		FileReader fr = new FileReader(sfile.getAbsoluteFile());

		BufferedReader br = new BufferedReader(fr);		

		String line;
		int lineCount=0;
		
		String[] name=sfile.getName().split("_");
		
		int r=Integer.parseInt(String.valueOf(name[2].charAt(7)));
		
		System.out.println(sfile.getName()+" R="+r);
		
		while ((line = br.readLine()) != null) {
			lineCount++;
			
			if(lineCount%r==0) wpage+=line+"\n";
			else{
				wpage+=line+"####";
			}
			
		}
		
		writeZFile(wpage, "_"+sfile.getName());
		
	}
	
	public void genSubs() throws IOException, InterruptedException{
		if (zfolder.exists()) {
			//System.out.println("TRUE Exists");
			int i=0;
			for (File zfile : zfolder.listFiles()) {				
				
				//System.out.println(zfile.getAbsolutePath());
				
				i++;
				
				if (i >= 3){
					continue;
				}else {					

					System.out.println("Reading... " + zfile.getName());
					String wpage = readZFile(zfile);

					System.out.println("Writing..");
					if (wpage.length() > 0)
						writeZFile(wpage,
								zfolder.getName() + "_" + zfile.getName()
										+ ".txt");

				}
				
				
				
			}
		}
	}
	
	public String readZFile(File zfile) throws IOException {
		FileReader fr = new FileReader(zfile.getAbsoluteFile());

		BufferedReader br = new BufferedReader(fr);

		String line;
		String wpage="";
		int count=0;

		while ((line = br.readLine()) != null) {
			String lline=line.replaceAll("//s", "");
			int pos=Integer.parseInt(lline);
			
			String att=this.attributes.get(pos);
			att=att.replaceAll(" ", "");
			
			LinkedHashSet<String> values=this.attributesValues.get(att);
			int size=values.size();
			String[] avalues=new String[size];
			values.toArray(avalues);
			
			String rvalue=avalues[rand.nextInt(size)];
			
			String op=ops[rand.nextInt(ops.length)];
			
			wpage+=att+"::::"+op+"::::"+rvalue+"::::"+rand.nextGaussian()+"\n";
			
			System.out.println("Line: "+count++);
		}
		br.close();	
		
		return wpage;
		
	}
	
	public void writeZFile(String wpage, String outputFileName) throws IOException, InterruptedException{
		
		
		//String zpath="gen//GenSubscription//ZZIPF//"+outputFileName;	
		String zpath="gen//GenSubscription//SHUFFLED_ZZIPF//"+outputFileName;
		
		File afile = new File(zpath);					
		
		if (!afile.exists()) {
			afile.createNewFile();
		}
		
		FileWriter afw = new FileWriter(afile.getAbsoluteFile());
		BufferedWriter abw = new BufferedWriter(afw);
		
		abw.write(wpage);
		
		Thread.sleep(2000);
		
		abw.close();
	}
	
	public void print(){
		
		/*for(Entry<String, LinkedHashSet<String>> record:attributesValues.entrySet()){
			System.out.println(record.getKey());
			System.out.println(record.getValue().toString());
			System.out.println("+++++++++++++++++++++++++++++++++\n");
			break;
		}*/
		
		String wpage="";
		for(String attribute:attributes){
			attribute=attribute.replaceAll(" ", "");
			wpage+=attribute;
			System.out.println(attribute);
			for(String value:attributesValues.get(attribute)){
				wpage+="/n/t"+value;
			}
			wpage+="/n/n";
		}
		
		System.out.println(wpage);
	}

	public void start() {

		if (t == null) {
			t = new Thread(this);
			t.start();
		}
	}

}


public class SubGenerator {
	String _endpoint;
	
	public SubGenerator(String contextPath){
		
		_endpoint=contextPath;
				
		File folder = new File(_endpoint+"//ATTVALS");	
		
		System.out.println(folder.getAbsolutePath());

		for (File sfile : folder.listFiles()) {
			String apath=_endpoint+"//ATTS//"+sfile.getName();
			File afile=new File(apath);
			
			String fname=sfile.getName(); //Automotive.txt			
			String zpath=_endpoint+"//ZIPF//"+fname.substring(0, fname.length()-4);
			
			File zfolder = new File(zpath);
			System.out.println(zfolder.getAbsolutePath());
			
			SubThread mt = new SubThread(afile,sfile,zfolder,_endpoint);
			mt.start();
			//break;
		}
		
	}
	
	public static void main(String[] args) throws IOException{
		File folder = new File("gen//GenSubscription//SHUFFLED_ZZIPF");		

		for (File sfile : folder.listFiles()) {
			//String apath=sfile.getAbsolutePath();
			//File afile=new File(apath);
			
			//String fname=sfile.getName(); //Automotive.txt			
			//String zpath="gen//GenSubscription//ZIPF//"+fname.substring(0, fname.length()-4);
			//System.out.println(zpath);
			//File zfolder = new File(zpath);
			
			SubThread mt = new SubThread(sfile);
			mt.start();
			//break;
		}
		
		//SubGenerator subgen=new SubGenerator("gen//GenSubscription");
	}

}
