package com.pubsub.subindex;

public class Predicate {
	
	private String attribute;
	private String operator;
	private String value;
	private double weight;
	
	private String _predicate;
	
	public Predicate(String _predicate){		
		initPredicate(_predicate);
	}
	
	public void initPredicate(String _predicate){
		String[] _wblocks=_predicate.split(":",4);
		
		this._predicate=_predicate;
		
		this.attribute=_wblocks[0];	
		this.operator=_wblocks[1];		
		this.value=_wblocks[2];
		try{
			this.weight=Double.parseDouble(_wblocks[3]);
		}catch(NumberFormatException nfe){
			this.weight=0;
		}
	}

	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public String get_predicate() {
		return _predicate;
	}

	public void set_predicate(String _predicate) {
		initPredicate(_predicate);
	}

}
