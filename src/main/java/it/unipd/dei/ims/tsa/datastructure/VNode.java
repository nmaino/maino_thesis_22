package it.unipd.dei.ims.tsa.datastructure;

import it.unipd.dei.ims.rum.utilities.UrlUtilities;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.List;

/** Class used to deal with the virtual documents
 * in the TSA algorithm.
 * */
public class VNode {

	/** iri of the node*/
	private String iri;
	
	/** id of the node*/
	private int id;
	
	/** radius of the node*/
	private int radius;
	
	/** Statement with objects literals that describe the
	 * node*/
	private List<Statement> nodeCloud;
	
	private List<VNode> nxts;
	
	/** list of pairs with the VNode (iri node)
	 * that are subject of this one node and the statement that connects 
	 * them*/
	private List<Pair<Statement, VNode>> nxtsList;
	
	private VNode nxt;
	
	/** status of this node:
	 * 0: not visited
	 * 1: visited */
	private int status;
	
	/** previous node in the path*/
	private VNode previous;
	
	/** statement connecting to the previous node in the path */
	private Statement previousStatement;
	
	/** set to true if it is not necessary to check this node
	 * in the pruning process
	 * */
	private boolean toSpare;
	
	/** static weight of the path 
	 * in which the node is present.
	 * */
	private double staticPathWeight;
	
	public VNode() {
		this.nodeCloud = new ArrayList<Statement>();
		nxts = new ArrayList<VNode>();
		this.nxtsList = new ArrayList<Pair<Statement, VNode>>();
		toSpare = false;
		staticPathWeight = 0;
	}
	
	/** add a literal triple to this node */
	public void addTriple(Statement t) {
		this.nodeCloud.add(t);
	}
	
	public boolean isEqualByIri(VNode second) {
		if(this.getIri().equals(second.getIri()))
			return true;
		return false;
	}

	public String getIri() {
		return iri;
	}

	public void setIri(String iri) {
		this.iri = iri;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getRadius() {
		return radius;
	}

	public void setRadius(int radius) {
		this.radius = radius;
	}
	
	public String toString() {
		return this.iri;
	}


	public VNode getPrevious() {
		return previous;
	}

	public void setPrevious(VNode previous) {
		this.previous = previous;
	}

	public List<VNode> getNxts() {
		return nxts;
	}

	public void setNxts(List<VNode> nxts) {
		this.nxts = nxts;
	}
	
	public void addNxt(VNode nxt) {
		this.nxts.add(nxt);
	}
	
	public void addNxt(Statement t, VNode nxt) {
		Pair<Statement, VNode> p = new MutablePair<Statement, VNode>(t, nxt);
		this.nxtsList.add(p);
	}
	
	/** returns a String with the text contained in this
	 * node (the iri and the literal neighbors)*/
	public String getNodeText() {
		String nodeDoc = "";
		
		//take the accessory cloud of this node
		List<Statement> cloud = this.getNodeCloud();
		
		for(Statement t : cloud) {
			//take the text from the surrounding triples
			String pred = t.getPredicate().toString();
			String obj = t.getObject().stringValue();
			nodeDoc += " " + UrlUtilities.takeFinalWordFromIRI(pred);
			if(UrlUtilities.checkIfValidURL(obj)) {
				nodeDoc += " " + UrlUtilities.takeFinalWordFromIRI(obj);
			} else {
				nodeDoc += " " + obj;
			}
		}
		
		//get ALSO the neighbors
		/*I thought a little bit about this one
		 * nonetheless, it is necessary otherwise
		 * we would lost in the counting
		 * the words in the predicates
		 * */
		for(Pair<Statement, VNode> p : this.getNxtsList()) {
			Statement t = p.getLeft();
			String pred = t.getPredicate().toString();
			String obj = t.getObject().stringValue();
			nodeDoc += " " + UrlUtilities.takeFinalWordFromIRI(pred);
			if(UrlUtilities.checkIfValidURL(obj)) {
				nodeDoc += " " + UrlUtilities.takeFinalWordFromIRI(obj);
			} else {
				nodeDoc += " " + obj;
			}
		}
		
		String iri = this.getIri();
		nodeDoc += " " + UrlUtilities.takeFinalWordFromIRI(iri);
		
		return nodeDoc;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	/** Returns the cloud of literal nodes 
	 *of the node*/
	public List<Statement> getNodeCloud() {
		return nodeCloud;
	}

	public void setNodeCloud(List<Statement> nodeCloud) {
		this.nodeCloud = nodeCloud;
	}

	public VNode getNxt() {
		return nxt;
	}

	public void setNxt(VNode nxt) {
		this.nxt = nxt;
	}

	public List<Pair<Statement, VNode>> getNxtsList() {
		return nxtsList;
	}

	public void setNxtsList(List<Pair<Statement, VNode>> nxtsList) {
		this.nxtsList = nxtsList;
	}

	public Statement getPreviousStatement() {
		return previousStatement;
	}

	public void setPreviousStatement(Statement previousStatement) {
		this.previousStatement = previousStatement;
	}

	public boolean isToSpare() {
		return toSpare;
	}

	public void setToSpare(boolean toSpare) {
		this.toSpare = toSpare;
	}

	public double getStaticPathWeight() {
		return staticPathWeight;
	}

	public void setStaticPathWeight(double staticPathWeight) {
		this.staticPathWeight = staticPathWeight;
	}
}
