package com.pubsub.publisher;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import com.pubsub.subindex.Predicate;



public class PublicationFactory {
	
	private List<String> pubclass=new LinkedList<String>();
	private List<Publication> publications=new LinkedList<Publication>();

	public PublicationFactory(String pclass,BufferedReader in) throws IOException {
		updatePublications(pclass,in);
		this.pubclass.add(pclass);		
	}
	
	public PublicationFactory(Map<Integer,List<Publication>> counts) throws IOException {
		updatePublications(counts);
	}
	
	public void updatePublications(String pclass,BufferedReader in) throws IOException {
		String line;
		Publication pub=null;
		Predicate pred;
		String wpubdata="";
		
		while ((line = in.readLine()) != null) {
			if(line.startsWith("ITEM")){
				pub=new Publication(pclass);
				System.out.println("Adding new publication: "+line);
			}else if(line.length()>0 & line.contains("=")){				
				pred=new Predicate(line, "=", 2);
				if(pub!=null)
					pub.appendPredicate(pred);
				wpubdata+=line;
			}else{
				if(pub!=null){
					publications.add(pub);
					pub.setData(wpubdata);
					wpubdata="";
					
				}
			}
			
			if(publications.size()>=50) break;
			
		}
	}
	
	public void updatePublications(Map<Integer,List<Publication>> counts){
		for(Entry<Integer, List<Publication>> e:counts.entrySet()){
			publications.addAll(e.getValue());
		}
	}
	public int getFactorySize(){
		return publications.size();
	}
	
	public int getPClassSize(){
		return pubclass.size();
	}

	public Publication create() {
        Publication _pub = getRandomPublication();
        //String _pubclass = _pub.getPclass();

        //Publication pub = new Publication(resource,referee);

        return _pub;
    }
	
	/**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected String getRandomClass() {
        return pubclass.get(ThreadLocalRandom.current().nextInt(getPClassSize()));
    	
    }
    
    /**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected Publication getRandomPublication() {
        return publications.get(ThreadLocalRandom.current().nextInt(getFactorySize()));
    }

	public List<Publication> getPublications() {
		return publications;
	}
}
