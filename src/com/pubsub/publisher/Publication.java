package com.pubsub.publisher;

import java.util.LinkedList;
import java.util.List;

import com.pubsub.subindex.Predicate;

public class Publication {
	
	private String data;
	
	private long issuedTime;
	
	private double decayRelScore;
	
	private List<Predicate> predicates;
	
	private String pclass;
	
	private int publicationSize;
	
	/*public Publication(String data) {
		this.data=data;
		
		this.predicates=new LinkedList<Predicate>();
		
		String[] preds=data.split("####");
		for(int i=0;i<preds.length;i++){
			Predicate pred=new Predicate(preds[i], "=", 2);
			//System.out.println("\n Predicate: "+pred.getAttribute());
			this.predicates.add(pred);
		}
		
		issuedTime=System.currentTimeMillis();
	}*/
	
	public Publication(){}
	
	public Publication(String pclass) {		
		this.setPclass(pclass);
		this.predicates=new LinkedList<Predicate>();		
		this.issuedTime=System.currentTimeMillis();
	}
	
	public int getPublicationSize(){
		publicationSize=this.predicates.size();
		return publicationSize;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public long getIssuedTime() {
		return issuedTime;
	}

	public void setIssuedTime(long issuedTime) {
		this.issuedTime = issuedTime;
	}

	public double getDecayRelScore() {
		return decayRelScore;
	}

	public void setDecayRelScore(double decayRelScore) {
		this.decayRelScore = decayRelScore;
	}

	public List<Predicate> getPredicates() {
		return predicates;
	}
	
	public boolean appendPredicate(Predicate pred){
		return this.predicates.add(pred);
	}

	/**
	 * @return the pclass
	 */
	public String getPclass() {
		return pclass;
	}

	/**
	 * @param pclass the pclass to set
	 */
	public void setPclass(String pclass) {
		this.pclass = pclass;
	}

	public void setPublicationSize(int publicationSize) {
		this.publicationSize = publicationSize;
	}

}
