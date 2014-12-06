package com.pubsub.data;

import java.io.File;
import java.io.IOException;




class MultiThread extends Thread {
	private Thread t;	
	private double skew;
	private float bottom = 0;	
	private float[] zipfarr;	
	int ccount=0;
	private Utility utility;
	private int size;
	
	

	MultiThread(File file) throws IOException {
		utility = new Utility(file);		
		this.skew = 1;
		this.size = utility.getSize();
		this.zipfarr=new float[utility.getSize()];		
		
		System.out.println("Array Size: "+utility.getSize());
		
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
	   
	  utility.writeFile(wpage, "zipfRanks_N="+this.size+" r="+r+".gp", true);
	   
	   
	   System.out.println("Att Sum: "+att_sum);
	   System.out.println("Zipf Att Sum: "+all_att_sum);   
	  
	   
	
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
