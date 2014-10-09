package com.pubsub.model;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;



public class PublicationFactory {
	private List<String> resources;
	private List<String> referees;
	
	public PublicationFactory(List<String> resources,List<String> referees){
		if (resources == null || resources.isEmpty()) {
            throw new IllegalArgumentException("At least 1 resource is required");
        }
		if (referees == null || referees.isEmpty()) {
            throw new IllegalArgumentException("At least 1 resource is required");
        }
		this.resources = resources;	
		this.referees = referees;
	}

	public Publication create() {
        String resource = getRandomResource();
        String referee = getRandomReferee();

        Publication pub = new Publication(resource,referee);

        return pub;
    }
	
	/**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected String getRandomResource() {
        return resources.get(ThreadLocalRandom.current().nextInt(resources.size()));
    }
    
    /**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected String getRandomReferee() {
        return referees.get(ThreadLocalRandom.current().nextInt(referees.size()));
    }
}
