package com.pubsub.subindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A directed graph data structure.
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class Graph {
  /** Color used to mark unvisited nodes */
  public static final int VISIT_COLOR_WHITE = 1;

  /** Color used to mark nodes as they are first visited in DFS order */
  public static final int VISIT_COLOR_GREY = 2;

  /** Color used to mark nodes after descendants are completely visited */
  public static final int VISIT_COLOR_BLACK = 3;

  /** Vector<Vertex> of graph verticies */
  private List<Vertex> verticies;

  /** Vector<Edge> of edges in the graph */
  private List<Edge<Double>> edges;

  /** The vertex identified as the root of the graph */
  private Vertex rootVertex;

  /**
   * Construct a new graph without any vertices or edges
   */
  public Graph() {
    verticies = new ArrayList<Vertex>();
    edges = new ArrayList<Edge<Double>>();
  }

  /**
   * Are there any verticies in the graph
   * 
   * @return true if there are no verticies in the graph
   */
  public boolean isEmpty() {
    return verticies.size() == 0;
  }

  
  /**
   * Add a vertex to the graph
   * 
   * @param v
   *          the Vertex to add
   * @return true if the vertex was added, false if it was already in the graph.
   */
  public boolean addVertex(Vertex v) {
    boolean added=false;    
    
    if (findVertexByName(v.getName())==null) {
      added = verticies.add(v);
    }
    return added;
  }
  
  public boolean addVertexToSeqNo(int seqno,Vertex v) {
	    boolean added=false;    
	    //try{
	    if (findVertexByName(v.getName())==null) {
	      verticies.add(seqno,v);
	      added=true;
	      
	    }
	    /*}catch(IndexOutOfBoundsException iex){
	    	System.out.println("Index out|");
	    }*/
	    return added;
	  }

  /**
   * Get the vertex count.
   * 
   * @return the number of verticies in the graph.
   */
  public int size() {
    return verticies.size();
  }

  /**
   * Get the root vertex
   * 
   * @return the root vertex if one is set, null if no vertex has been set as
   *         the root.
   */
  public Vertex getRootVertex() {
    return rootVertex;
  }
  
  public void refreshGraph(){
	  for(Vertex v:verticies) v.clearMatch();
  }

  /**
   * Set a root vertex. If root does no exist in the graph it is added.
   * 
   * @param root -
   *          the vertex to set as the root and optionally add if it does not
   *          exist in the graph.
   */
  public void setRootVertex(Vertex root) {
    this.rootVertex = root;
    if (verticies.contains(root) == false)
      this.addVertex(root);
  }

  /**
   * Get the given Vertex.
   * 
   * @param n
   *          the index [0, size()-1] of the Vertex to access
   * @return the nth Vertex
   */
  public Vertex getVertex(int n) {
    return verticies.get(n);
  }

  /**
   * Get the graph verticies
   * 
   * @return the graph verticies
   */
  public List<Vertex> getVerticies() {
    return this.verticies;
  }

  /**
   * Insert a directed, weighted Edge<Double> into the graph.
   * 
   * @param from -
   *          the Edge<Double> starting vertex
   * @param to -
   *          the Edge<Double> ending vertex
   * @param d -
   *          the Edge<Double> weight/cost
   * @return true if the Edge<Double> was added, false if from already has this Edge<Double>
   * @throws IllegalArgumentException
   *           if from/to are not verticies in the graph
   */
  public boolean addEdge(Vertex from, Vertex to, double d) throws IllegalArgumentException {
    if (findVertexByName(from.getName())==null)
      throw new IllegalArgumentException("from is not in graph");
    if (findVertexByName(to.getName())==null)
      throw new IllegalArgumentException("to is not in graph");

    Edge<Double> e = new Edge(from, to, d);
    if (from.findEdge(to) != null)
      return false;
    else {
      from.addEdge(e);
      to.addEdge(e);
      edges.add(e);
      return true;
    }
  }

  /**
   * Insert a bidirectional Edge<Double> in the graph
   * 
   * @param from -
   *          the Edge<Double> starting vertex
   * @param to -
   *          the Edge<Double> ending vertex
   * @param ratio -
   *          the Edge<Double> weight/cost
   * @return true if edges between both nodes were added, false otherwise
   * @throws IllegalArgumentException
   *           if from/to are not verticies in the graph
   */
  public boolean insertBiEdge(Vertex from, Vertex to, double ratio)
      throws IllegalArgumentException {
    return addEdge(from, to, ratio) && addEdge(to, from, ratio);
  }

  /**
   * Get the graph edges
   * 
   * @return the graph edges
   */
  public List<Edge<Double>> getEdges() {
    return this.edges;
  }

  /**
   * Remove a vertex from the graph
   * 
   * @param v
   *          the Vertex to remove
   * @return true if the Vertex was removed
   */
  public boolean removeVertex(Vertex v) {
    if (!verticies.contains(v))
      return false;

    verticies.remove(v);
    if (v == rootVertex)
      rootVertex = null;

    // Remove the edges associated with v
    for (int n = 0; n < v.getOutgoingEdgeCount(); n++) {
      Edge<Double> e = v.getOutgoingEdge(n);
      v.remove(e);
      Vertex to = e.getTo();
      to.remove(e);
      edges.remove(e);
    }
    for (int n = 0; n < v.getIncomingEdgeCount(); n++) {
      Edge<Double> e = v.getIncomingEdge(n);
      v.remove(e);
      Vertex predecessor = e.getFrom();
      predecessor.remove(e);
    }
    return true;
  }

  /**
   * Remove an Edge<Double> from the graph
   * 
   * @param from -
   *          the Edge<Double> starting vertex
   * @param to -
   *          the Edge<Double> ending vertex
   * @return true if the Edge<Double> exists, false otherwise
   */
  public boolean removeEdge(Vertex from, Vertex to) {
    Edge<Double> e = from.findEdge(to);
    if (e == null)
      return false;
    else {
      from.remove(e);
      to.remove(e);
      edges.remove(e);
      return true;
    }
  }

  /**
   * Clear the mark state of all verticies in the graph by calling clearMark()
   * on all verticies.
   * 
   * @see Vertex#clearMark()
   */
  public void clearMark() {
    for (Vertex w : verticies)
      w.clearMark();
  }

  /**
   * Clear the mark state of all edges in the graph by calling clearMark() on
   * all edges.
   */
  public void clearEdges() {
    for (Edge<Double> e : edges)
      e.clearMark();
  }

  /**
   * Perform a depth first serach using recursion.
   * 
   * @param v -
   *          the Vertex to start the search from
   * @param visitor -
   *          the vistor to inform prior to
   * @see Visitor#visit(Graph, Vertex)
   */
  public void depthFirstSearch(Vertex v, final Visitor visitor) {
    VisitorEX<Predicate, RuntimeException> wrapper = new VisitorEX<Predicate, RuntimeException>() {
      public void visit(Graph g, Vertex v) throws RuntimeException {
        if (visitor != null)
          visitor.visit(g, v);
      }
    };
    this.depthFirstSearch(v, wrapper);
  }

  /**
   * Perform a depth first search using recursion. The search may be cut short
   * if the visitor throws an exception.
   * 
   * @param <E>
   * 
   * @param v -
   *          the Vertex to start the search from
   * @param visitor -
   *          the vistor to inform prior to
   * @see Visitor#visit(Graph, Vertex)
   * @throws E
   *           if visitor.visit throws an exception
   */
  public <E extends Exception> void depthFirstSearch(Vertex v, VisitorEX<Predicate, E> visitor) throws E {
    if (visitor != null)
      visitor.visit(this, v);
    v.visit();
    for (int i = 0; i < v.getOutgoingEdgeCount(); i++) {
      Edge<Double> e = v.getOutgoingEdge(i);
      if (!e.getTo().visited()) {
        depthFirstSearch((Vertex)e.getTo(), visitor);
      }
    }
  }

  /**
   * Perform a breadth first search of this graph, starting at v.
   * 
   * @param v -
   *          the search starting point
   * @param visitor -
   *          the vistor whose vist method is called prior to visting a vertex.
   */
  public void breadthFirstSearch(Vertex v, final Visitor visitor) {
    VisitorEX<Predicate, RuntimeException> wrapper = new VisitorEX<Predicate, RuntimeException>() {
      public void visit(Graph g, Vertex v) throws RuntimeException {
        if (visitor != null)
          visitor.visit(g, v);
      }
    };
    this.breadthFirstSearch(v, wrapper);
  }

  /**
   * Perform a breadth first search of this graph, starting at v. The vist may
   * be cut short if visitor throws an exception during a vist callback.
   * 
   * @param <E>
   * 
   * @param v -
   *          the search starting point
   * @param visitor -
   *          the vistor whose vist method is called prior to visting a vertex.
   * @throws E
   *           if vistor.visit throws an exception
   */
  public <E extends Exception> void breadthFirstSearch(Vertex v, VisitorEX<Predicate, E> visitor)
      throws E {
    LinkedList<Vertex> q = new LinkedList<Vertex>();

    q.add(v);
    if (visitor != null)
      visitor.visit(this, v);
    v.visit();
    while (q.isEmpty() == false) {
      v = q.removeFirst();
      for (int i = 0; i < v.getOutgoingEdgeCount(); i++) {
        Edge<Double> e = v.getOutgoingEdge(i);
        Vertex to = (Vertex)e.getTo();
        if (!to.visited()) {
          q.add(to);
          if (visitor != null)
            visitor.visit(this, to);
          to.visit();
        }
      }
    }
  }

  /**
   * Find the spanning tree using a DFS starting from v.
   * 
   * @param v -
   *          the vertex to start the search from
   * @param visitor -
   *          visitor invoked after each vertex is visited and an edge is added
   *          to the tree.
   */
  public void dfsSpanningTree(Vertex v, DFSVisitor visitor) {
    v.visit();
    if (visitor != null)
      visitor.visit(this, v);

    for (int i = 0; i < v.getOutgoingEdgeCount(); i++) {
      Edge<Double> e = v.getOutgoingEdge(i);
      if (!e.getTo().visited()) {
        if (visitor != null)
          visitor.visit(this, v, e);
        
        e.mark();
        dfsSpanningTree((Vertex) e.getTo(), visitor);
      }
    }
  }
  
	public void sortEdgesMaxMin() {
		for (int i = 0; i < edges.size(); i++) {
			double minWeightEdge = edges.get(i).getCost();
			int index = i;
			for (int j = i + 1; j < edges.size(); j++) {
				if (edges.get(j).getCost() >= minWeightEdge) {
					minWeightEdge = edges.get(j).getCost();
					index = j;
				}
			}
			if (index != i) {
				Edge temp = edges.get(index);
				edges.set(index, edges.get(i));
				edges.set(i, temp);				
			}
		}
	}

	public double computeMaxMST() {
		this.sortEdgesMaxMin();
		HashSet<Edge> mst = new HashSet<Edge>();
		for (int i = 0; i < edges.size(); i++) {
			if (!edges.get(i).areNodesVisited()) {
				mst.add(edges.get(i));
				edges.get(i).setNodesVisited();
			}
		}
		Object[] newEdge = mst.toArray();
		double totalCost = 0;
		for (int i = 0; i < newEdge.length; i++) {
			totalCost += ((Edge) newEdge[i]).getCost();
		}
		return totalCost;
	}
	
	public double computeAverageEdgeWeight(){
		double sumWeight=0;
		for(Edge e:this.getEdges()){
			//System.out.print(e.getCost()+" ");
			sumWeight+=e.getCost();
		}
		//System.out.println(sumWeight);
		double avgWeight=sumWeight/this.getEdges().size();
		return avgWeight;
	}
  


  /**
   * Search the verticies for one with name.
   * 
   * @param name -
   *          the vertex name
   * @return the first vertex with a matching name, null if no matches are found
   */
  public Vertex findVertexByName(String name) {
    Vertex match = null;
    for (Vertex v : verticies) {
      if (name.equals(v.getName())) {
        match = v;
        break;
      }
    }
    return match;
  }

  /**
   * Search the verticies for one with data.
   * 
   * @param data -
   *          the vertex data to match
   * @param compare -
   *          the comparator to perform the match
   * @return the first vertex with a matching data, null if no matches are found
   */
  public Vertex findVertexByData(Predicate pred, Comparator compare) {
    Vertex match = null;
    for (Vertex v : verticies) {
      if (compare.compare(pred, v.getPred()) == 0) {
        match = v;
        break;
      }
    }
    return match;
  }

  /**
   * Search the graph for cycles. In order to detect cycles, we use a modified
   * depth first search called a colored DFS. All nodes are initially marked
   * white. When a node is encountered, it is marked grey, and when its
   * descendants are completely visited, it is marked black. If a grey node is
   * ever encountered, then there is a cycle.
   * 
   * @return the edges that form cycles in the graph. The array will be empty if
   *         there are no cycles.
   */
  public Edge<Double>[] findCycles() {
    ArrayList<Edge<Double>> cycleEdges = new ArrayList<Edge<Double>>();
    // Mark all verticies as white
    for (int n = 0; n < verticies.size(); n++) {
      Vertex v = getVertex(n);
      v.setMarkState(VISIT_COLOR_WHITE);
    }
    for (int n = 0; n < verticies.size(); n++) {
      Vertex v = getVertex(n);
      visit(v, cycleEdges);
    }

    Edge<Double>[] cycles = new Edge[cycleEdges.size()];
    cycleEdges.toArray(cycles);
    return cycles;
  }

  private void visit(Vertex v, ArrayList<Edge<Double>> cycleEdges) {
    v.setMarkState(VISIT_COLOR_GREY);
    int count = v.getOutgoingEdgeCount();
    for (int n = 0; n < count; n++) {
      Edge<Double> e = v.getOutgoingEdge(n);
      Vertex u = (Vertex)e.getTo();
      if (u.getMarkState() == VISIT_COLOR_GREY) {
        // A cycle Edge<Double>
        cycleEdges.add(e);
      } else if (u.getMarkState() == VISIT_COLOR_WHITE) {
        visit(u, cycleEdges);
      }
    }
    v.setMarkState(VISIT_COLOR_BLACK);
  }

  public String toString() {
    StringBuffer tmp = new StringBuffer("Graph[");
    for (Vertex v : verticies){
      tmp.append(v);
      String seqno=String.valueOf(v.getSeqno());
      tmp.append(" seqno: "+seqno);
      tmp.append("\n");
    }
    tmp.append(']');
    System.out.println("Size: "+verticies.size());
    return tmp.toString();
  }
  
	public String toStringWithFlag() {
		StringBuffer tmp = new StringBuffer("Graph[");
		for (Vertex v : verticies) {
			tmp.append(v+" Match Flag: "+v.isMatch());
			String seqno=String.valueOf(v.getSeqno());
		    tmp.append(" seqno: "+seqno);
			tmp.append("\n");
		}
		tmp.append(']');
		System.out.println("Size: " + verticies.size());
		return tmp.toString();
	}

}


/**
 * A directed, weighted edge in a graph
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 */
class Edge<Double> {
  private Vertex from;

  private Vertex to;

  private double cost;

  private boolean mark;

  /**
   * Create a zero cost edge between from and to
   * 
   * @param from
   *          the starting vertex
   * @param to
   *          the ending vertex
   */
  public Edge(Vertex from, Vertex to) {
    //this(from, to, 0);
  }

  /**
   * Create an edge between from and to with the given cost.
   * 
   * @param from2
   *          the starting vertex
   * @param to2
   *          the ending vertex
   * @param d
   *          the cost of the edge
   */
  public Edge(Vertex from2, Vertex to2, double d) {
    this.from = from2;
    this.to = to2;
    this.cost = d;
    mark = false;
  }

  /**
   * Get the ending vertex
   * 
   * @return ending vertex
   */
  public Vertex getTo() {
    return to;
  }

  /**
   * Get the starting vertex
   * 
   * @return starting vertex
   */
  public Vertex getFrom() {
    return from;
  }

  /**
   * Get the cost of the edge
   * 
   * @return cost of the edge
   */
  public double getCost() {
    return cost;
  }

  /**
   * Set the mark flag of the edge
   * 
   */
  public void mark() {
    mark = true;
  }

  /**
   * Clear the edge mark flag
   * 
   */
  public void clearMark() {
    mark = false;
  }

  /**
   * Get the edge mark flag
   * 
   * @return edge mark flag
   */
  public boolean isMarked() {
    return mark;
  }
  
	public void setNodesVisited() {
		from.visit();
		to.visit();
	}

	public boolean areNodesVisited() {
		return (from.visited() & to.visited());
	}

  /**
   * String rep of edge
   * 
   * @return string rep with from/to vertex names and cost
   */
  public String toString() {
    StringBuffer tmp = new StringBuffer("Edge[from: ");
    tmp.append(from.getName());
    tmp.append(",to: ");
    tmp.append(to.getName());
    tmp.append(", cost: ");
    tmp.append(cost);
    tmp.append("]");
    return tmp.toString();
  }
}

/**
 * A named graph vertex with optional data.
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 */
//@SuppressWarnings("unchecked")
class Vertex {
  private List<Edge<Double>> incomingEdges;

  private List<Edge<Double>> outgoingEdges;

  private String name;

  private boolean mark;

  private int markState;

  private Predicate pred;  
  
  private boolean match=false;
  
  private int seqno;

  /**
   * Calls this(null, null).
   */
  public Vertex() {
    this(null, null);
  }

  /**
   * Create a vertex with the given name and no data
   * 
   * @param n
   */
  public Vertex(String n) {
    this(n, null);
  }

  /**
   * Create a Vertex with name n and given data
   * 
   * @param n -
   *          name of vertex
   * @param pred -
   *          data associated with vertex
   */
  public Vertex(String n, Predicate pred) {
    incomingEdges = new ArrayList<Edge<Double>>();
    outgoingEdges = new ArrayList<Edge<Double>>();
    name = n;
    mark = false;
    this.pred = pred;   
  }

  /**
   * @return the possibly null name of the vertex
   */
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
	    this.name=name;
	  }

	public Predicate getPred() {
		return this.pred;
	}
	
	public void setPred(Predicate pred) {
		this.pred = pred;
	}

  

  /**
   * Add an edge to the vertex. If edge.from is this vertex, its an outgoing
   * edge. If edge.to is this vertex, its an incoming edge. If neither from or
   * to is this vertex, the edge is not added.
   * 
   * @param e -
   *          the edge to add
   * @return true if the edge was added, false otherwise
   */
  public boolean addEdge(Edge<Double> e) {
    if (e.getFrom() == this)
      outgoingEdges.add(e);
    else if (e.getTo() == this)
      incomingEdges.add(e);
    else
      return false;
    return true;
  }

  /**
   * Add an outgoing edge ending at to.
   * 
   * @param to -
   *          the destination vertex
   * @param cost
   *          the edge cost
   */
  public void addOutgoingEdge(Vertex to, int cost) {
    Edge<Double> out = new Edge(this, to, cost);
    outgoingEdges.add(out);
  }

  /**
   * Add an incoming edge starting at from
   * 
   * @param from -
   *          the starting vertex
   * @param cost
   *          the edge cost
   */
  public void addIncomingEdge(Vertex from, int cost) {
    Edge<Double> out = new Edge(this, from, cost);
    incomingEdges.add(out);
  }

  /**
   * Check the vertex for either an incoming or outgoing edge mathcing e.
   * 
   * @param e
   *          the edge to check
   * @return true it has an edge
   */
  public boolean hasEdge(Edge<Double> e) {
    if (e.getFrom() == this)
      return incomingEdges.contains(e);
    else if (e.getTo() == this)
      return outgoingEdges.contains(e);
    else
      return false;
  }

  /**
   * Remove an edge from this vertex
   * 
   * @param e -
   *          the edge to remove
   * @return true if the edge was removed, false if the edge was not connected
   *         to this vertex
   */
  public boolean remove(Edge<Double> e) {
    if (e.getFrom() == this)
      incomingEdges.remove(e);
    else if (e.getTo() == this)
      outgoingEdges.remove(e);
    else
      return false;
    return true;
  }

  /**
   * 
   * @return the count of incoming edges
   */
  public int getIncomingEdgeCount() {
    return incomingEdges.size();
  }

  /**
   * Get the ith incoming edge
   * 
   * @param i
   *          the index into incoming edges
   * @return ith incoming edge
   */
  public Edge<Double> getIncomingEdge(int i) {
    return incomingEdges.get(i);
  }

  /**
   * Get the incoming edges
   * 
   * @return incoming edge list
   */
  public List getIncomingEdges() {
    return this.incomingEdges;
  }

  /**
   * 
   * @return the count of incoming edges
   */
  public int getOutgoingEdgeCount() {
    return outgoingEdges.size();
  }

  /**
   * Get the ith outgoing edge
   * 
   * @param i
   *          the index into outgoing edges
   * @return ith outgoing edge
   */
  public Edge<Double> getOutgoingEdge(int i) {
    return outgoingEdges.get(i);
  }

  /**
   * Get the outgoing edges
   * 
   * @return outgoing edge list
   */
  public List getOutgoingEdges() {
    return this.outgoingEdges;
  }

  /**
   * Search the outgoing edges looking for an edge whose's edge.to == dest.
   * 
   * @param dest
   *          the destination
   * @return the outgoing edge going to dest if one exists, null otherwise.
   */
  public Edge<Double> findEdge(Vertex dest) {
    for (Edge<Double> e : outgoingEdges) {
      if (e.getTo() == dest)
        return e;
    }
    return null;
  }

  /**
   * Search the outgoing edges for a match to e.
   * 
   * @param e -
   *          the edge to check
   * @return e if its a member of the outgoing edges, null otherwise.
   */
  public Edge<Double> findEdge(Edge<Double> e) {
    if (outgoingEdges.contains(e))
      return e;
    else
      return null;
  }

  /**
   * What is the cost from this vertext to the dest vertex.
   * 
   * @param dest -
   *          the destination vertex.
   * @return Return Integer.MAX_VALUE if we have no edge to dest, 0 if dest is
   *         this vertex, the cost of the outgoing edge otherwise.
   */
  public double cost(Vertex dest) {
    if (dest == this)
      return 0;

    Edge<Double> e = findEdge(dest);
    double cost = Double.MAX_VALUE;
    if (e != null)
      cost = e.getCost();
    return cost;
  }

  /**
   * Is there an outgoing edge ending at dest.
   * 
   * @param dest -
   *          the vertex to check
   * @return true if there is an outgoing edge ending at vertex, false
   *         otherwise.
   */
  public boolean hasEdge(Vertex dest) {
    return (findEdge(dest) != null);
  }

  /**
   * Has this vertex been marked during a visit
   * 
   * @return true is visit has been called
   */
  public boolean visited() {
    return mark;
  }

  /**
   * Set the vertex mark flag.
   * 
   */
  public void mark() {
    mark = true;
  }

  /**
   * Set the mark state to state.
   * 
   * @param state
   *          the state
   */
  public void setMarkState(int state) {
    markState = state;
  }

  /**
   * Get the mark state value.
   * 
   * @return the mark state
   */
  public int getMarkState() {
    return markState;
  }

  /**
   * Visit the vertex and set the mark flag to true.
   * 
   */
  public void visit() {
    mark();
  }

  /**
   * Clear the visited mark flag.
   * 
   */
  public void clearMatch() {
    match = false;
  }
  
  /**
   * Clear the visited mark flag.
   * 
   */
  public void clearMark() {
    mark = false;
  }

  /**
   * @return a string form of the vertex with in and out edges.
   */
  public String toString() {
    StringBuffer tmp = new StringBuffer("Vertex(");
    tmp.append(name);
    tmp.append(", data=");
    tmp.append(((com.pubsub.subindex.Predicate) pred).get_predicate());
    tmp.append("), in:[");
    for (int i = 0; i < incomingEdges.size(); i++) {
      Edge<Double> e = incomingEdges.get(i);
      if (i > 0)
        tmp.append(',');
      tmp.append('{');
      tmp.append(e.getFrom().name);
      tmp.append(',');
      tmp.append(e.getCost());
      tmp.append('}');
    }
    tmp.append("], out:[");
    for (int i = 0; i < outgoingEdges.size(); i++) {
      Edge<Double> e = outgoingEdges.get(i);
      if (i > 0)
        tmp.append(',');
      tmp.append('{');
      tmp.append(e.getTo().name);
      tmp.append(',');
      tmp.append(e.getCost());
      tmp.append('}');
    }
    tmp.append(']');
    return tmp.toString();
  }

public boolean isMatch() {
	return match;
}

public void setMatch(boolean match) {
	this.match = match;
}

public int getSeqno() {
	return seqno;
}

public void setSeqno(int seqno) {
	this.seqno = seqno;
}



}

/**
 * A graph visitor interface.
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 */
interface Visitor {
  /**
   * Called by the graph traversal methods when a vertex is first visited.
   * 
   * @param g -
   *          the graph
   * @param v -
   *          the vertex being visited.
   */
  public void visit(Graph g, Vertex v);
}

/**
 * A graph visitor interface that can throw an exception during a visit
 * callback.
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 * @param <E>
 */
interface VisitorEX<Predicate, E extends Exception> {
  /**
   * Called by the graph traversal methods when a vertex is first visited.
   * 
   * @param g -
   *          the graph
   * @param v -
   *          the vertex being visited.
   * @throws E
   *           exception for any error
   */
  public void visit(Graph g, Vertex v) throws E;
}

/**
 * A spanning tree visitor callback interface
 * 
 * @see Graph#dfsSpanningTree(Vertex, DFSVisitor)
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 * @param <T>
 */
interface DFSVisitor {
  /**
   * Called by the graph traversal methods when a vertex is first visited.
   * 
   * @param g -
   *          the graph
   * @param v -
   *          the vertex being visited.
   */
  public void visit(Graph g, Vertex v);

  /**
   * Used dfsSpanningTree to notify the visitor of each outgoing edge to an
   * unvisited vertex.
   * 
   * @param g -
   *          the graph
   * @param v -
   *          the vertex being visited
   * @param e -
   *          the outgoing edge from v
   */
  public void visit(Graph g, Vertex v, Edge<Double> e);
}