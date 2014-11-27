package com.pubsub.subindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;
 
public class InvertedIndex {
 
	int seqno=0;
	
	Map<String, List<Tuple>> index = new HashMap<String, List<Tuple>>();
	
	Graph graph=new Graph();	
 
	public void indexFile(File file) throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			List<Vertex> subPredicates=new LinkedList<Vertex>();
			String[] _predicates=line.split("#");
			for (int i=0;i<_predicates.length;i++) {
				String _predicate=_predicates[i];	
				//_predicate.replaceFirst("\\s", "");				
					
				String[] _wblocks=_predicate.split(":");
				
				String attribute=_wblocks[0];
				//attribute.replaceAll("\\s","");
				
				String operator=_wblocks[1];
				
				//System.out.println("Length: "+_wblocks.length);
				String value=_wblocks[2];
				String preference=_wblocks[3];
				
				double weight=0;
				try{
					weight=Double.parseDouble(preference);
				}catch(NumberFormatException nfe){
					weight=0;
				}
				
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
								subPredicates.add(pred);
								
								
								
								break search;
							}
						}

						
						// Operator Found, but no predicate found, so adding new predicate
						Predicate newpred=new Predicate(_predicate);
						Vertex newvertex=new Vertex(newpred.get_predicate(), newpred);
						newvertex.setSeqno(seqno);
						
						tidx.appendPredicate(newvertex);
						subPredicates.add(newvertex);
						
						
						graph.addVertexToSeqNo(seqno, newvertex);
						
						System.out.println("SeqNo: "+seqno);
						seqno++;
					}
				}
				
				// Operator not found, adding new operator posting list with new predicate 
				if(!found){
					Predicate pred=new Predicate(_predicate);
					Vertex vertex=new Vertex(pred.get_predicate(), pred);
					vertex.setSeqno(seqno);
					
					Tuple ttidx=new Tuple(operator, vertex);
					idx.add(ttidx);	
					subPredicates.add(vertex);
					
					
					graph.addVertexToSeqNo(seqno,vertex);
					
					System.out.println("SeqNo: "+seqno);
					seqno++;
				}
				
				
			}
			
			//updateGraph(subPredicates);
			updateEdge(subPredicates);
			
		}		
		
		printIndex();
		
		System.out.println(graph.toString());
	}
	
//	public void updateGraph(List<Predicate> _predicates){
//		//String _predicate;
//		//Map<String,Double> edgecost=new HashMap<String, Double>();
//		
//		for(Predicate pred:_predicates){		
////			
////			String attribute=pred.getAttribute();	
////			String operator=pred.getOperator();		
////			String value=pred.getValue();
////			double weight=pred.getWeight();
//			
//			Vertex vertex=new Vertex(pred.get_predicate(), pred);
//			//Vertex<String> vertex=new Vertex<String>(pred.get_predicate(),pred);
//			
//			boolean added=graph.addVertex(vertex);
//			//System.out.println("Added: "+added+" "+pred.get_predicate());			
//			
//			//edgecost.put(_predicate, Double.parseDouble(preference));
//		}		
//		
//		//System.out.println("+++++++++++");
//		updateEdge(_predicates);
//		//System.out.println("+++++++++++");
//		
//	}
	
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
				   
				   Double pref1=v1.getPred().getWeight();
				   Double pref2=v2.getPred().getWeight();
				   
				   
				   //System.out.println("##"+v1.getPred().get_predicate());
				   double ratio=pref1/pref2;
				   
				   if(ratio==1){
					   graph.insertBiEdge(v2, v1, ratio);
				   }else if(ratio>1){
					   graph.addEdge(v2, v1, ratio);
				   }else{
					   ratio=pref2/pref1;
					   graph.addEdge(v1, v2, ratio);
				   }
				   
				   
				   
			   }
				   //System.out.print(_predicate+",");
			   
			   //System.out.println();
		   }
		
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
					wpage+="\n\t\t["+data+"]";
				}
				//System.out.println(attribute+"->"+operator+"->"+_predicates.size());
				wpage+="\n\t]";
			}
			wpage+="\n]";
		}
		
		System.out.println(wpage);
	}
	
	
 
	public boolean search(String _predicate) {
			String[] _wblocks=_predicate.split(":",4);
			List<Tuple> idx = index.get(_wblocks[0]);
			if (idx != null) {
				for (Tuple t : idx) {
					if(t.getOperator().equals(_wblocks[1])){
						for(Vertex pred:t.get_predicates()){
							Vertex imvertex = pred;
							if(imvertex.getPred().get_predicate().equals(_predicate)){
								
								System.out.println("Matched: ");
								
								Vertex gmvertex=graph.getVerticies().get(pred.getSeqno());
								Predicate gmvpred=gmvertex.getPred();
								
								gmvertex.setMatch(true);
								imvertex.setMatch(true);								
								
								String gmpredicate=gmvpred.get_predicate();
								
								System.out.println("Graph:"+gmpredicate);
								System.out.println("IIx:"+pred.getPred().get_predicate());
								
								System.out.println(graph.toStringWithFlag());
								
								return true;
							}
						}						
					}
					
				}
			}
			
			
			
			return false;		
	}
 
	public static void main(String[] args) {
		try {
			InvertedIndex idx = new InvertedIndex();
			
			idx.indexFile(new File("gen//OrderedAttributes//TestSub2.txt"));
			
			double maxMST=idx.getGraph().computeMaxMST();
			
			System.out.println("MAX MST WEIGHT: "+maxMST);
			
			String publication="ProductGroup:=:31707:0.9#Feature:>:127428:0.3";
			
			String[] pub=publication.split("#");
			double foundCount=0;
			
			for(int i=0;i<pub.length;i++){
				boolean success=idx.search("ProductGroup:=:31707:0.9");
				
				System.out.println("Found Flag: "+pub[i]+" "+success);
				
				if(success) foundCount++;				
			}
			
			System.out.println("Relevancy score :"+(maxMST*(foundCount/idx.getGraph().getVerticies().size()))+" "+publication+" ");
			
//			Vertex v1=new Vertex("Hello");
//			
//			Vertex v2=new Vertex("Hi");
//			
//			List<Vertex> list1=new LinkedList<Vertex>();
//			
//			List<Vertex> list2=new LinkedList<Vertex>();
//			
//			list1.add(0,v1);
//			list1.add(1,v2);
//			
//			list2.add(0,v1);
//			
//			System.out.println(list1.contains(list2.get(0)));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
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
