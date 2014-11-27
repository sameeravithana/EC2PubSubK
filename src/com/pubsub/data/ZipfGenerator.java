package com.pubsub.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;




class MultiThread extends Thread {
	private Thread t;
	private File file;
	private int size;
	private double skew;
	private float bottom = 0;
	private LinkedList<String> atts;
	private float[] zipfarr;
	private Random rnd = new Random();
	int ccount=0;
	

	MultiThread(File file) throws IOException {
		this.file = file;		
		this.skew = 1;
		this.atts = readFile();
		//this.size=5;
		this.size = this.atts.size();
		this.zipfarr=new float[this.size];
		
		//for(int i=1;i<=this.zipfarr.length;i++) this.zipfarr[i-1]=i;
		
		System.out.println("Array Size: "+this.size);
		
		updateBottom(this.size);
		
		genNormalZipf();
		
	}
	
	public void updateBottom(int csize){
		for (int i = 1; i <= csize; i++) {
			this.bottom += (1 / Math.pow(i, this.skew));
		}		
	}
	// This method returns a probability that the given rank occurs.
	public float getProbability(int rank) {		
		return (float) ((1.0f / Math.pow(rank, this.skew)) / this.bottom);
	}
	
	
	
	
	
	public void generateDist(int r) throws IOException, InterruptedException{	   
	   String wpage="";
	   long att_freqs[]=new long[this.size];
	   
	   //Map<Integer,Long> hatt_freq=new HashMap<Integer, Long>();
	   
	   //List<char[]> all_atts=new ArrayList<char[]>();
	   
	   long comb_sum=choose(r);
	   
	   long att_sum=comb_sum * r;	  
	   
	   long all_att_sum=0;
	   
	   for(int i=0;i<this.size;i++){		   
		   long freqs=(long) Math.ceil(att_sum * zipfarr[i]);
		   att_freqs[i]=freqs;
		   //hatt_freq.put(i, freqs);
		   all_att_sum+=freqs;
		   for(long j=0;j<freqs;j++){
			   //all_atts.add(Character.toChars(i));
			   wpage+=String.valueOf(i)+"\n";
		   }		   
		   
		   System.out.println("Rank: "+i+" Freq: "+freqs);
	   }
	   
	  writeFile(wpage, "zipfRanks_N="+this.size+" r="+r+".gp", true);
	   
	   
	   System.out.println("Att Sum: "+att_sum);
	   System.out.println("Zipf Att Sum: "+all_att_sum);   
	  
	   
//	   List<Integer> keys = new ArrayList(hatt_freq.keySet());
//	   
//	   
//	   Collections.shuffle(keys);
//	   
//	   boolean flag=true;
//	   
//	   for(int i=0;i<keys.size();i+=r){
//		   int j=0;
//		   while(j<r){
//			   wpage+=(i+j)+",";
//			   if(hatt_freq.get(i+j)<=0){
//				   flag=false;
//			   }			   
//			   j++;
//		   }
//		   
//		   if(flag){
//			   int k=0;
//			   while(k<r){
//				   wpage+=(i+k)+",";
//				   hatt_freq.put(i+k,hatt_freq.get(i+k)-1);	   
//				   k++;
//			   }
//			   
//		   }
//		   wpage+="\n";
//		   
//	   }
//	   
//	   
//	   //System.out.println(att_freqs.toString());
//	   
//	   int[] tuples=new int[r];
//	   boolean flag=true;
//	   while(flag){
//		   for(int i=0;i<tuples.length;i++){
//			   tuples[i]=rnd.nextInt(r);
//			   if(att_freqs[tuples[i]]<=0){
//				   flag=false
//				   break;			   
//			   }
//		   }
//	   }
	   
	   
	   
	  
	 
	 
		
		
	
	}	
	
	
	
 	public long choose(int r) {
		int n = this.size;

		long[][] C = new long[n + 1][r + 1];

		int i, j;

		// Caculate value of Binomial Coefficient in bottom up manner
		for (i = 0; i <= n; i++) {
			for (j = 0; j <= min(i, r); j++) {
				// Base Cases
				if (j == 0 || j == i)
					C[i][j] = 1;

				// Calculate value using previosly stored values
				else
					C[i][j] = C[i - 1][j - 1] + C[i - 1][j];
			}
		}

		return C[n][r];
	}

	int min(int a, int b) {
		return (a < b) ? a : b;
	}
		   
	public void genNormalZipf(){
		
		for(int i=0;i<this.size;i++){
			int rank=i+1;
			float rprob=getProbability(rank);
			this.zipfarr[i]=rprob;			
		}

	}
	
	
	
	public LinkedList<String> readFile() throws IOException{
		FileReader fr = new FileReader(file.getAbsoluteFile());
		
		BufferedReader br = new BufferedReader(fr);		
	
		String line;
		LinkedList<String> atts=new LinkedList<String>();
		while((line=br.readLine())!=null){			
				String[] tuple = line.split("=", 2);
				String att = tuple[0];
				att.replaceAll("\\s+","");
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


	public void run() {
		try {
//			generateDist(2);
//			generateDist(3);
			generateDist(4);
//			generateDist(5);
//			generateDist(6);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public void start() {

		if (t == null) {
			t = new Thread(this);
			t.start();
		}
	}

}

public class ZipfGenerator {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		
		File folder = new File("gen//OrderedAttributes2");
		
		//File file=new File("PlotData//log_zipf.gp");

		for (File file : folder.listFiles()) {
			MultiThread mt = new MultiThread(file);
			mt.start();
			//break;
		}
	}
}
