package com.pubsub.compute.diversity.lsh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.pubsub.publisher.Publication;
import com.pubsub.subindex.Predicate;

public class LSH {
	
	int numHashes_m;
	int dimensions;	
	int numPubs=0;
	double similarityT;
	int numHashTables_L;
	int numRows_r;
	
	
	Map<String, HashMap<Integer, ArrayList<Boolean>>> pubMatrix; // characteristic matrix; dimension * publications
	List<Publication> pubs; // list of all publications
	
	List<ArrayList<Integer>> signatureMatrix;
	List<ArrayList<Integer>> hashes;
	
	Map<Integer,
		HashMap<Integer,ArrayList<Integer>>
	> lshIndex;
	
	
	public LSH(List<Publication> pubs, double error, double sthreshold){
		this.pubs=pubs;
		this.numPubs=pubs.size();
		this.numHashes_m = (int) (1/ (error * error));
		this.similarityT=sthreshold;
		
		
		pubMatrix=new HashMap<String ,HashMap<Integer,ArrayList<Boolean>>>();
		/*pubHashes=new double[pubs.size()][numHashesK];
		targetBucket=new boolean[pubs.size()][numHashesK];*/
		
		
		buildSetMatrix();
		printSetMatrix();
		System.out.println("------------------------------------\n");
		
		signatureMatrix=new ArrayList<ArrayList<Integer>>();
		hashes=new ArrayList<ArrayList<Integer>>();
		
		
		System.out.println("Number of minhashes to generate: "+numHashes_m);
		buildSignatureMatrix();
		printSigMatrix();
		System.out.println("------------------------------------\n");
		
		lshIndex=new HashMap<Integer,
				HashMap<Integer,ArrayList<Integer>>
		>();
		
		System.out.println("LSH Hash parameters are initiating..., Given similarity threshold: "+similarityT);
		initiateHashTables();
		System.out.println("LSH Hash parameters L="+numHashTables_L+" r="+numRows_r);
		computeLSH();
		printLSHIndex();
	}
	
	
	private void computeLSH() {
		
		for(int pubpos=0;pubpos<signatureMatrix.size();pubpos++){
			int table=0;
			ArrayList<Integer> _hashes=signatureMatrix.get(pubpos);
			for(int cols=0;cols<_hashes.size();cols+=numRows_r){
				int pubr[]=new int[numRows_r];
				int j=0;
				for(int i=cols;i<cols+numRows_r;i++){
					pubr[j]=_hashes.get(i);
					//System.out.print(pubr[j]+" ");
					j++;
				}
				//System.out.println();
				
				int hashBucket=lshH(pubr);
				
				HashMap<Integer,ArrayList<Integer>> mBuckets;
				ArrayList<Integer> t;
				if(lshIndex.get(table)==null){
					mBuckets=new HashMap<Integer, ArrayList<Integer>>();
						t=new ArrayList<Integer>();
						t.add(pubpos);
					mBuckets.put(hashBucket, t);
					lshIndex.put(table, mBuckets);
				}else{
					mBuckets=lshIndex.get(table);
						if(mBuckets.get(hashBucket)==null){
							t=new ArrayList<Integer>();
						}else{
							t=mBuckets.get(hashBucket);
						}
						t.add(pubpos);
					mBuckets.put(hashBucket, t);
					lshIndex.put(table, mBuckets);
				}
				
				table++;
			}
		}
		
	}


	/**
	 * Similarity threshold s
	 * s= (1/L)^(1/r) -> (1)
	 * Lr=m -> (2) 
	 */
	private void initiateHashTables() {	
		double distance=Double.MAX_VALUE;
		double lrT=0;
		for (int i = 2; i < numHashes_m; i++) {
			if (numHashes_m % i == 0) {				
				lrT=getLRThreshold(i,numHashes_m / i);				
				if(distance >= Math.abs(lrT-similarityT)){
					numHashTables_L = i;
					numRows_r = numHashes_m / i;
					distance=Math.abs(lrT-similarityT);
				}				
			}
		}
		System.out.println("L="+numHashTables_L+" r="+numRows_r+" Set at Lr similarity threshold at "+similarityT);
	}
		
	private double getLRThreshold(int L,int r){
		//System.out.println("\t "+(1/(double)numHashTables_L)+" "+(1/(double)numRows_r));
		return Math.pow((1/(double)L), (1/(double)r));
	}
	
	
	
	private ArrayList<Boolean> updateFlags(ArrayList<Boolean> flags, int jpubs){
		for(int i=0;i<jpubs;i++){
			if(i<flags.size()) continue;
			else{
				flags.add(i, false);
			}
		}
		return flags;
	}
	
	
	private void buildSetMatrix() { 
		int jpubs=0;
		int did=0;
		for(Publication pub:pubs){
	        for(Predicate pred:pub.getPredicates()){
	        	String predicate=pred.get_predicate();
	        	if(pubMatrix.get(predicate)==null){
	        		ArrayList<Boolean> flags=new ArrayList<Boolean>();
	        		flags=updateFlags(flags, jpubs);	        		
	        		flags.add(jpubs, true);
	        		
	        		HashMap<Integer,ArrayList<Boolean>> dmap=new HashMap<Integer,ArrayList<Boolean>>();	        		
	        		dmap.put(did, flags);
	        		
	        		pubMatrix.put(predicate, dmap);
	        		did++;	        		
	        	}else{
	        		HashMap<Integer,ArrayList<Boolean>> cdmap=pubMatrix.get(predicate);
	        		int cid=-1;
	        		for(Entry<Integer, ArrayList<Boolean>> e:cdmap.entrySet()){
	        			cid=e.getKey();break;
	        		}
	        		if(cid>=0){
	        			ArrayList<Boolean> cflags=cdmap.get(cid);
	        			updateFlags(cflags, jpubs);
	        			cflags.add(jpubs, true);
	        			cdmap.put(cid, cflags);
	        			pubMatrix.put(predicate, cdmap);
	        		}			
	        		
	        	}
	        	
	        }
	        //pubs.add(jpubs, pub);
	        jpubs++;
	        //printSetMatrix();
		}  
		
		dimensions=did;
    }
	
	private void buildSignatureMatrix(){
		// Refer signatureMatrix=new int[numPubs][numHashes_m];
		for(int i=0;i<numPubs;i++){
			ArrayList<Integer> hlist=new ArrayList<Integer>();
			for(int j=0;j<numHashes_m;j++){
				hlist.add(j, Integer.MAX_VALUE);				
			}
			signatureMatrix.add(i,hlist);
		}
		System.out.println("SIG matrix initiated: [publications: "+numPubs+"][numHashes: "+numHashes_m+"]");
		//printSigMatrix();
		System.out.println();
		
		//System.out.println("Initiating Hashes");
		initHashes();
		//System.out.println("Initiated Hashes\n");
		
		for(String predicate:pubMatrix.keySet()){
			HashMap<Integer,ArrayList<Boolean>> cdmap=pubMatrix.get(predicate);    		
    		for(Entry<Integer, ArrayList<Boolean>> e:cdmap.entrySet()){
    			int cd=e.getKey();
    			int pubpos=0;
    			for(boolean pubflag:e.getValue()){
    				//System.out.print(" Pub: ["+pubpos+"] Flag: "+pubflag );
    				if(pubflag){
    					ArrayList<Integer> _hashes=hashes.get(cd);
    					//System.out.println("Hash length: "+signatureMatrix.get(pubpos).size());
    					int u=0;
    					ArrayList<Integer> _oldHashes=null;
    					for(int h:_hashes){
    						_oldHashes=signatureMatrix.get(pubpos);
    						if(h<_oldHashes.get(u)){
    							_oldHashes.set(u,h);    							
    						}
    						u++;
    					}
    					signatureMatrix.set(pubpos, _oldHashes);
    					
    				}
    				pubpos++;
    			}
    			//System.out.println();
    			break;
    		}
		}
		
	}
	private void initHashes(){	
		// Refer hashes=new int[dimensions][numHashes_m];
		int i=0,j;
		while(i<dimensions){
			j=0;
			//System.out.print("Dimension: ["+i+"] ");
			ArrayList<Integer> _hashes=new ArrayList<Integer>();
			while(j<numHashes_m){
				int hash=(h1(i)+j*h2(i)) % dimensions;
				_hashes.add(j,hash);
				//System.out.print(hash+" ");
				j++;
			}
			hashes.add(i,_hashes);
			i++;
			//System.out.println();
		}
			
	}
	
	private int h1(int num){
		return num + 1 % dimensions;
	}
	
	private int h2(int num){
		return 3*num + 1 % dimensions;
	}
	
	private int lshH(int[] num){
		int ret=0,j=0;
		for(int i=0;i<num.length;i++){
			ret+=num[i]*Math.pow(10, j);
		}
		return ret;
	}	
	
	private void printSetMatrix(){
		System.out.println("++++++ Characteristic Matrix +++++++");
		for(String attribute:pubMatrix.keySet()){
			System.out.print(attribute+" ");
			HashMap<Integer,ArrayList<Boolean>> cdmap=pubMatrix.get(attribute);
			for(Entry<Integer, ArrayList<Boolean>> e:cdmap.entrySet()){
				System.out.print(e.getKey()+" [");
				for(boolean flag:e.getValue()){
					if(flag) System.out.print("1 ");
					else{
						System.out.print("0 ");
					}
				}
			}
			System.out.println();
		}
		
	}
	
	private void printSigMatrix(){
		System.out.println("++++++ Signature Matrix +++++++");
		int pubpos=0;
		for(List<Integer> _hashes:signatureMatrix){
			System.out.print("Publication: ["+pubpos+"] ");
			for(int _hash:_hashes){
				System.out.print(_hash+" ");
			}
			System.out.println();
			pubpos++;
		}
	}
	
	private void printLSHIndex(){
		//ArrayList<Map<Integer,List<Integer>>>();
		System.out.println("\n++++++ LSH Index +++++++");
		int table;
		for (Entry<Integer, HashMap<Integer, ArrayList<Integer>>> cmap : lshIndex
				.entrySet()) {
			table = cmap.getKey();
			System.out.println("LSH Table: " + table);
			HashMap<Integer, ArrayList<Integer>> dmap = cmap.getValue();
			for (Entry<Integer, ArrayList<Integer>> e : dmap.entrySet()) {
				int bucket = e.getKey();
				System.out.print("\t Bucket: " + bucket + " Publications [");
				for (int pub : e.getValue()) {
					System.out.print(pub + " ");
				}
				System.out.println("] ");
			}

		}
	}
	
	public static void main(String[] args){
		Publication pub1=new Publication("Elec");
		Predicate pred11=new Predicate("Brand=iRulu", "=", 2);
		Predicate pred12=new Predicate("Color=White", "=", 2);
		Predicate pred13=new Predicate("Manu=Apple", "=", 2);
		pub1.appendPredicate(pred11);
		pub1.appendPredicate(pred12);
		pub1.appendPredicate(pred13);
		
		Publication pub2=new Publication("Auto");
		Predicate pred21=new Predicate("Brand=iRulu", "=", 2);
		Predicate pred22=new Predicate("Color=White", "=", 2);
		Predicate pred23=new Predicate("Manu=Nokia", "=", 2);
		pub2.appendPredicate(pred21);
		pub2.appendPredicate(pred22);
		pub2.appendPredicate(pred23);
		
		Publication pub3=new Publication("Auto");		
		Predicate pred31=new Predicate("Brand=iRulu", "=", 2);
		Predicate pred32=new Predicate("Color=Red", "=", 2);
		Predicate pred33=new Predicate("Manu=Apple", "=", 2);
		pub3.appendPredicate(pred31);
		pub3.appendPredicate(pred32);
		pub3.appendPredicate(pred33);
		
		Publication pub4=new Publication("Auto");		
		Predicate pred41=new Predicate("Brand=Verizon", "=", 2);
		Predicate pred42=new Predicate("Color=Red", "=", 2);
		Predicate pred43=new Predicate("Manu=Apple", "=", 2);
		pub4.appendPredicate(pred41);
		pub4.appendPredicate(pred42);
		pub4.appendPredicate(pred43);
		
		List<Publication> pubs=new ArrayList<Publication>();
		pubs.add(pub1);
		pubs.add(pub2);
		pubs.add(pub3);
		pubs.add(pub4);
		
		double error=0.2;
		double sthreshold=0.85;
		LSH lsh=new LSH(pubs, error,sthreshold);
		
		//lsh.primeFactors(100);
		//System.out.println(Integer.MAX_VALUE);	
		
	}

}
