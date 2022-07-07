package it.unipd.dei.ims.tsa.datastructure;

import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Second tentative, after VPath,
 * to create an object that describes a path inside
 * a virtual graph*/
public class VirtualPath {

	/** Nodes creating the path*/
	private List<VNode> nodes;
	
	
	/** Indicates if it is necessary to delete this path */
	private boolean toDelete;
	
	
	public VirtualPath() {
		this.nodes = new ArrayList<VNode>();
	}
	
	public VirtualPath(VirtualPath oldPath) {
		//copy the nodes
		this.nodes = new ArrayList<VNode>(oldPath.getNodes());
	}
	
	public void addNode(VNode addendum) {
		this.nodes.add(addendum);
	}
	
	public void pruneWithQueryWords(List<String> queryWords) {
		VNode leaf = this.getTail();
		//get the triples of this node
		VNode n = leaf;
		while(n != null) {
			List<Statement> cloud = n.getNodeCloud();
			Iterator<Statement> iter = cloud.iterator();
			while(iter.hasNext()) {
				Statement t = iter.next();
				boolean ok = BlazegraphUsefulMethods.containsQueryWord(t, queryWords);
				if(!ok) {
					iter.remove();
				}
			}
			
			VNode old = n;
			n = n.getPrevious();
			
			cloud = n.getNodeCloud();
			if(cloud.size() == 0) {
				//in this case, delete this node from the path (prune)
				old.setPrevious(null);
				if(n!=null)
					n.setNxt(null);
				this.nodes.remove(old);
			}
		}
	}
	
	public List<VNode> getNodes() {
		return nodes;
	}

	public void setNodes(List<VNode> nodes) {
		this.nodes = nodes;
	}

	public VNode getHead() {
		return this.nodes.get(0);
	}


	public VNode getTail() {
		return this.nodes.get(nodes.size() - 1);
	}


	public boolean isToDelete() {
		return toDelete;
	}

	public void setToDelete(boolean toDelete) {
		this.toDelete = toDelete;
	}
	
	public String toString() {
		String s = "";
		for(VNode node : this.nodes) {
			s += node.toString() + " ";
		}
		return s;
	}
}
