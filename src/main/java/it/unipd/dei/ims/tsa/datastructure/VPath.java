package it.unipd.dei.ims.tsa.datastructure;

import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import it.unipd.dei.ims.rum.utilities.UrlUtilities;
import it.unipd.dei.ims.terrier.utilities.TerrierUsefulMethods;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.List;

public class VPath {

	/** List of statements forming the path. 
	 * From the source to the target.
	 * */
	private List<Statement> pathTriples;
	
	/** IRI of the first node of the path*/
	private String head;
	
	/** IRI of the last node of the path */
	private String tail;
	
	/** to decide if we need to delete this path from 
	 * a computation or not.
	 * */
	private boolean toDelete;

	public VPath() {
		pathTriples = new ArrayList<Statement>();
	}
	
	
	public void addTriple(Statement t) {
		//set the head if necessary
		if(head==null)
			head = t.getSubject().toString();
		//add the triple
		pathTriples.add(t);
		//set the new tail
		tail = t.getObject().toString();
	}
	
	public List<Statement> getPathTriples() {
		return pathTriples;
	}
	
	public void copyFromPath(VPath p) {
		this.pathTriples = new ArrayList<Statement>(p.getPathTriples());
		this.head = p.head;
		this.tail = p.tail;
	}

	public void setPathTriples(List<Statement> pathTriples) {
		this.pathTriples = pathTriples;
	}
	
	/** Check if the path contains at least one query word.
	 * 
	 * @param queryWords list of query words already processed
	 * by Terrier.
	 * */
	public boolean containsQueryWords(List<String> queryWords) {
		for(int i = 0; i<this.pathTriples.size(); ++i) {
			List<String> tripleWords = null;
			if(i==0) {
				//special treatment for the root, we don't count
				//it (otherwise all paths would meet the requirement)
				Statement triple = pathTriples.get(i);
				String pred = triple.getPredicate().toString();
				String obj = triple.getObject().stringValue();
				String tripleDoc = "";
				tripleDoc = UrlUtilities.takeFinalWordFromIRI(pred);
				if(UrlUtilities.checkIfValidURL(obj)) {
					tripleDoc += " " + UrlUtilities.takeFinalWordFromIRI(obj);
				} else {
					tripleDoc += " " + obj;
				}
				tripleWords = 
						TerrierUsefulMethods.
						getDocumentWordsWithTerrierAsList(tripleDoc);
			} else {
				Statement triple = pathTriples.get(i);
				String tripleDoc = BlazegraphUsefulMethods.fromStatementToDocument(triple);
				tripleWords = 
						TerrierUsefulMethods.
						getDocumentWordsWithTerrierAsList(tripleDoc);
			}
			
			for(String queryWord : queryWords) {
				//for each query word
				if(tripleWords.contains(queryWord))
					return true;
			}				
		}
		return false;
	}
	
	/** Removes from the path, starting from the leaf,
	 * the triples which don't contain any query word.
	 * */
	public void pruneWithQueryWords(List<String> queryWords) {
		int size = this.pathTriples.size();
		for(int i = size - 1; i>=0; --i ) {
			//get the words composing the triple
			Statement t = this.pathTriples.get(i);
			String tripleDoc = BlazegraphUsefulMethods.fromStatementToDocument(t);
			List<String> tripleWordsList = 
					TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(tripleDoc);
			//intersection with the query words
			tripleWordsList.retainAll(queryWords);
			if(tripleWordsList.size()==0) {
				//this means this triple doesn't contain any query word
				//therefore we eliminate it
				this.pathTriples.remove(i);
				
			}
		}
	}


	public String getHead() {
		return head;
	}


	public void setHead(String head) {
		this.head = head;
	}


	public String getTail() {
		return tail;
	}


	public void setTail(String tail) {
		this.tail = tail;
	}


	public boolean isToDelete() {
		return toDelete;
	}


	public void setToDelete(boolean toDelete) {
		this.toDelete = toDelete;
	}
	
	
	
}
