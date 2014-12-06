package com.pubsub.subindex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.pubsub.publisher.Publication;
 
public class InvertedIndex {
 
	private static final long MEGABYTE = 1024L * 1024L;
	int seqno=0;
	
	Map<String, List<Tuple>> index = new HashMap<String, List<Tuple>>();
	
	Graph graph=new Graph();

	private double avgWeight;	
	
	String lineSplit="####";
	String predSplit="::::";
	
	int u=0;
	
	long start;
	
	public String wpage="";
	
	public InvertedIndex(){
		start=System.currentTimeMillis();
	}
	
	public static long bytesToMegabytes(long bytes) {
	    return bytes / MEGABYTE;
	  }
 
	/**
	 * Index a subscription space by attribute & operator
	 * @param file File with all subscriptions
	 * @throws IOException
	 */
	public void indexFile(File file) throws IOException {
		
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			List<Vertex> subPredicates=new LinkedList<Vertex>();
			String[] _predicates=line.split(lineSplit);
			
			
			
			for (int i=0;i<_predicates.length;i++) {
				String _predicate=_predicates[i];	
				//_predicate.replaceFirst("\\s", "");				
					
				String[] _wblocks=_predicate.split(predSplit);
				
				String attribute=_wblocks[0];
				//attribute.replaceAll("\\s","");
				
				String operator=_wblocks[1];
				
				//System.out.println("Length: "+_wblocks.length);
				String value=_wblocks[2];
				String preference=_wblocks[3];
				
				//System.out.println("Pref "+preference);
				
				double weight=0;
				try{
					weight=Double.parseDouble(preference);
				}catch(NumberFormatException nfe){
					weight=0;
					//nfe.printStackTrace();
				}
				
				//System.out.println("Reading weight: "+weight);
				
				List<Tuple> idx = index.get(attribute);
				if (idx == null) {
					idx = new LinkedList<Tuple>();
					index.put(attribute, idx);
				}
				
				
				boolean found=false;
				
				
				search: for (Tuple tidx : idx) {
					if (tidx.getOperator().equals(operator)) {						
						for (Vertex pred : tidx.get_predicates()) {
							// Predicate found as it is
							if (pred.getPred().get_predicate().equals(_predicate)) {
								found = true;
								subPredicates.add(pred);
								break search;
							} 
							// Predicate found, but have to update preference
							else if (pred.getPred().getAttribute().equals(attribute)
									&& pred.getPred().getValue().equals(value)
									&& pred.getPred().getWeight() != weight) {
								Vertex v=graph.findVertexByName(pred.getPred().get_predicate());
								Predicate gpred=v.getPred();
								gpred.set_predicate(_predicate);
								v.setName(_predicate);
								
								pred.getPred().set_predicate(_predicate);
								found = true;
								
								//subPredicates.add(v);
								
								
								
								break search;
							}
						}

						
						// Operator Found, but no predicate found, so adding new predicate
						Predicate newpred=new Predicate(_predicate,predSplit,4);
						Vertex newvertex=new Vertex(newpred.get_predicate(), newpred);
						newvertex.setSeqno(seqno);
						
						tidx.appendPredicate(newvertex);
						subPredicates.add(newvertex);
						
						
						graph.addVertexToSeqNo(seqno, newvertex);
						
						//System.out.println("SeqNo: "+seqno);
						seqno++;
						
						found=true;
					}
				}
				
				// Operator not found, adding new operator posting list with new predicate 
				if(!found){
					Predicate pred=new Predicate(_predicate,predSplit,4);
					Vertex vertex=new Vertex(pred.get_predicate(), pred);
					vertex.setSeqno(seqno);
					
					Tuple ttidx=new Tuple(operator, vertex);
					idx.add(ttidx);	
					subPredicates.add(vertex);					
					
					graph.addVertexToSeqNo(seqno,vertex);
					
					
					seqno++;
				}
				
				//System.out.println("SeqNo: "+seqno);
				
				
			}
			
			
			updateEdge(subPredicates);
			
			u++;
			//System.out.println(u);
		}		
		
		long end=System.currentTimeMillis();
		
		System.out.println("Index construction time: "+(end-start)+" at subscription size: "+u);
		
		wpage+=String.valueOf(u)+" "+String.valueOf(end-start)+"\n";
		//printIndex();
		
		//System.out.println(graph.toString());
	}
	
	public void indexFile(BufferedReader reader) throws IOException {
		
		
		//BufferedReader reader = new BufferedReader(new FileReader(file));
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			List<Vertex> subPredicates=new LinkedList<Vertex>();
			String[] _predicates=line.split(lineSplit);
			
			
			
			for (int i=0;i<_predicates.length;i++) {
				String _predicate=_predicates[i];	
				//_predicate.replaceFirst("\\s", "");				
					
				String[] _wblocks=_predicate.split(predSplit);
				
				String attribute=_wblocks[0];
				//attribute.replaceAll("\\s","");
				
				String operator=_wblocks[1];
				
				//System.out.println("Length: "+_wblocks.length);
				String value=_wblocks[2];
				String preference=_wblocks[3];
				
				//System.out.println("Pref "+preference);
				
				double weight=0;
				try{
					weight=Double.parseDouble(preference);
				}catch(NumberFormatException nfe){
					weight=0;
					//nfe.printStackTrace();
				}
				
				//System.out.println("Reading weight: "+weight);
				
				List<Tuple> idx = index.get(attribute);
				if (idx == null) {
					idx = new LinkedList<Tuple>();
					index.put(attribute, idx);
				}
				
				
				boolean found=false;
				
				
				search: for (Tuple tidx : idx) {
					if (tidx.getOperator().equals(operator)) {						
						for (Vertex pred : tidx.get_predicates()) {
							// Predicate found as it is
							if (pred.getPred().get_predicate().equals(_predicate)) {
								found = true;
								subPredicates.add(pred);
								break search;
							} 
							// Predicate found, but have to update preference
							else if (pred.getPred().getAttribute().equals(attribute)
									&& pred.getPred().getValue().equals(value)
									&& pred.getPred().getWeight() != weight) {
								Vertex v=graph.findVertexByName(pred.getPred().get_predicate());
								Predicate gpred=v.getPred();
								gpred.set_predicate(_predicate);
								v.setName(_predicate);
								
								pred.getPred().set_predicate(_predicate);
								found = true;
								
								//subPredicates.add(v);
								
								
								
								break search;
							}
						}

						
						// Operator Found, but no predicate found, so adding new predicate
						Predicate newpred=new Predicate(_predicate,predSplit,4);
						Vertex newvertex=new Vertex(newpred.get_predicate(), newpred);
						newvertex.setSeqno(seqno);
						
						tidx.appendPredicate(newvertex);
						subPredicates.add(newvertex);
						
						
						graph.addVertexToSeqNo(seqno, newvertex);
						
						//System.out.println("SeqNo: "+seqno);
						seqno++;
						
						found=true;
					}
				}
				
				// Operator not found, adding new operator posting list with new predicate 
				if(!found){
					Predicate pred=new Predicate(_predicate,predSplit,4);
					Vertex vertex=new Vertex(pred.get_predicate(), pred);
					vertex.setSeqno(seqno);
					
					Tuple ttidx=new Tuple(operator, vertex);
					idx.add(ttidx);	
					subPredicates.add(vertex);					
					
					graph.addVertexToSeqNo(seqno,vertex);
					
					
					seqno++;
				}
				
				//System.out.println("SeqNo: "+seqno);
				
				
			}
			
			
			updateEdge(subPredicates);
			
			u++;
			//System.out.println(u);
		}		
		
		long end=System.currentTimeMillis();
		
		System.out.println("Index construction time: "+(end-start)+" at subscription size: "+u);
		
		wpage+=String.valueOf(u)+" "+String.valueOf(end-start)+"\n";
		//printIndex();
		
		//System.out.println(graph.toString());
	}
	
	public void writeFile(String outputFileName) throws IOException{
		String folderPath = "plot//";

		String filePath = folderPath+outputFileName;
		// String filePath="gen//ZipfAttributes//LocalZipf//"+outputFileName;
		File afile = new File(filePath);

		if (!afile.exists()) {
			afile.createNewFile();
		}

		FileWriter afw = new FileWriter(afile.getAbsoluteFile());
		BufferedWriter abw = new BufferedWriter(afw);

		abw.write(wpage);

		abw.close();
	}
	
	public void updateEdge(List<Vertex> _predicates){		
		   //String[] spredicates=new String[_predicates.size()];
			Vertex[] spredicates=new Vertex[_predicates.size()];
			
		   int i=0;
		   for(Vertex vertex:_predicates){
			   	//Predicate pred=vertex.getPred();
				spredicates[i]=vertex;
				i++;
		   }
		
		   // Create the initial vector
		   ICombinatoricsVector<Vertex> initialVector = Factory.createVector(spredicates);

		   // Create a simple combination generator to generate 3-combinations of the initial vector
		   Generator<Vertex> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);

		   // Print all possible combinations
		   for (ICombinatoricsVector<Vertex> combination : gen) {
			   if(combination.getVector().size()==2){
				   Vertex v1=combination.getValue(0);
				   Vertex v2=combination.getValue(1);
				   
				   //System.out.print(v1.getPred().get_predicate()+ "   "+v2.getPred().get_predicate()+"\n");
				   
				   Double pref1=v1.getPred().getWeight();
				   Double pref2=v2.getPred().getWeight();
				   
				   //System.out.println("*"+pref1+" "+pref2);
				   double diff=Math.abs(pref1-pref2);
				   double ratio;
				   
				   if(pref1>=pref2){
					   ratio=pref1/diff;
					   graph.addEdge(v2, v1, ratio);
				   }else{
					   ratio=pref2/diff;
					   graph.addEdge(v1, v2, ratio);
				   }
				   
				   /*double ratio=pref1/pref2;
				   
				   if(ratio==1){
					   graph.insertBiEdge(v2, v1, ratio);
				   }else if(ratio>1){					   
					   graph.addEdge(v2, v1, ratio);
				   }else{
					   ratio=pref2/pref1;					   
					   graph.addEdge(v1, v2, ratio);
				   }
				   */
				   
				   
			   }
				   //System.out.print(_predicate+",");
			   
			   //System.out.println();
		   }
		   
		   
			
		   //maxMST=graph.computeMaxMST();
		   setAvgWeight(graph.computeAverageEdgeWeight());
		
	}
		
	public void printIndex(){
		String wpage="";
		
		for(Entry<String, List<Tuple>> e:index.entrySet()){
			String attribute=e.getKey();
			wpage+="\n["+attribute;
			List<Tuple> tidx=e.getValue();
			for(Tuple t:tidx){
				String operator=t.getOperator();
				wpage+="\n\t["+operator;
				List<Vertex> _predicates=t.get_predicates();
				for(Vertex pred:_predicates){
					String data=pred.getPred().get_predicate();
					String seqno=String.valueOf(pred.getSeqno());
					wpage+="\n\t\t["+data+" seqno: "+seqno+"]";
				}
				//System.out.println(attribute+"->"+operator+"->"+_predicates.size());
				wpage+="\n\t]";
			}
			wpage+="\n]";
		}
		
		System.out.println(wpage);
	}
		
	
	/**
	 * Matching publication against inverted-index subscription graph & compute a relevancy score
	 * @param rpublication Publication to be matched
	 */	
	public double matchPublication(Publication rpublication){		
		
		long start=System.currentTimeMillis();
		
		double foundCount=0;
		double decayRel=0;
		
		for(Predicate pred:rpublication.getPredicates()){
			
			boolean success=search(pred);
			
			System.out.println("Found Flag: "+pred.get_predicate()+" "+success);
			
			if(success) foundCount++;			
		}
		
		if(foundCount>0){
			decayRel=computeDecayRelevancyScore(rpublication.getIssuedTime(), foundCount);
			
			rpublication.setDecayRelScore(decayRel);
			
			System.out.println("Relevancy score :"+rpublication.getDecayRelScore()+" Issued Time: "+rpublication.getIssuedTime()+" Content-> "+rpublication.getData());
			
		}
		
		long end=System.currentTimeMillis();
		System.out.println("Matching publication time: "+(end-start));
		
		graph.refreshGraph();
		
		return decayRel;
		
	}
			
	public boolean search(Predicate rpred) {
		
		List<Tuple> idx = index.get(rpred.getAttribute());
		if (idx != null) {
			for (Tuple t : idx) {
				//if(t.getOperator().equals(rpred.getOperator())){
				String op=t.getOperator();
				
					for(Vertex imvertex:t.get_predicates()){
						//Vertex imvertex = pred;
						//System.out.println("Index: "+imvertex.getPred().getValue()+" Pub_Predicate: "+rpred.getValue());
						if(validate(op,imvertex.getPred().getValue(),rpred.getValue())){
							
							//System.out.println("\n+++Matched: "+imvertex.getSeqno());
							
							Vertex gmvertex=graph.getVerticies().get(imvertex.getSeqno());
							Predicate gmvpred=gmvertex.getPred();
							
							gmvertex.setMatch(true);
							imvertex.setMatch(true);								
							
							String gmpredicate=gmvpred.get_predicate();
							
							//System.out.println("Graph:"+gmpredicate);
							//System.out.println("IIx:"+pred.getPred().get_predicate());
							
							//System.out.println(graph.toStringWithFlag());
							
							return true;
						}
					}						
				//}
				
			}
		}
		
		
		
		return false;		
}
	 
	public boolean validate(String op, String svalue, String pvalue){
		boolean flag=true;
		
		switch(op){
		case "po":flag=svalue.startsWith(pvalue);break;
		case "so":flag=svalue.endsWith(pvalue);break;
		case "eq":flag=svalue.equals(pvalue);break;
		}
		
		return flag;
		
	}

	public double computeDecayRelevancyScore(long pubIssuedTime,double foundCount){
		double relevancy=getAvgWeight()*(foundCount/graph.size());
		//System.out.println(Math.pow(2, 30)/pubIssuedTime);
		double decay=Math.exp(Math.pow(2, 30)/pubIssuedTime);
		return relevancy * decay;
	}
	
		
	public static void main(String[] args) {
		try {
			InvertedIndex idx=new InvertedIndex();
			
			File folder = new File("gen//OriginalSubscriptions");		

			int j=0;
			
			for (File subfile : folder.listFiles()) {

				System.out.println("Generating index: " + subfile.getName());

				//idx=new InvertedIndex();
				
				idx.indexFile(subfile);

				System.out.println("AVG WEIGHT: " + idx.getAvgWeight() + "\n");
				
				/*j++;
				
				if(j==2) break;*/
			}
			
			//idx.writeFile("Index_construction_r=2_3_continuous_idx.gp");			
			
			//Single publication
			/*String publication="OperatingSystem=Windows 7 Ultimate####Feature=Motormite Door Hinge Pin and Bushing Kit 38437";			
			
			Publication rpublication=new Publication(publication);
			
			idx.matchPublication(rpublication);*/			

			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*// Get the Java runtime
	    Runtime runtime = Runtime.getRuntime();
	    // Run the garbage collector
	    runtime.gc();
	    // Calculate the used memory
	    long memory = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used memory is bytes: " + memory);
	    System.out.println("Used memory is megabytes: "
	        + bytesToMegabytes(memory));*/
	    
	}
	
	public int getSeqno() {
		return seqno;
	}

	public Map<String, List<Tuple>> getIndex() {
		return index;
	}

	public Graph getGraph() {
		return graph;
	}
	
	/*public double getMaxMST() {
		return maxMST;
	}

	public void setMaxMST(double maxMST) {
		this.maxMST = maxMST;
	}*/
 
	public double getAvgWeight() {
		return avgWeight;
	}

	public void setAvgWeight(double avgWeight) {
		this.avgWeight = avgWeight;
	}

	private class Tuple {
		private String operator;		
		private List<Vertex> _predicates=new LinkedList<Vertex>();
 
		public Tuple(String operator, Vertex pred) {
			this.operator = operator;			
			
			this._predicates.add(pred);			
		}

		public String getOperator() {
			return operator;
		}

		public void setOperator(String operator) {
			this.operator = operator;
		}

		public List<Vertex> get_predicates() {
			return _predicates;
		}
		
		public void appendPredicate(Vertex pred){
			this._predicates.add(pred);
		}

		public void set_predicates(List<Vertex> _predicates) {
			this._predicates = _predicates;
		}
		
		public int getPredicatesSize(){
			return get_predicates().size();
		}

		
	}

	

	
	
	
}
