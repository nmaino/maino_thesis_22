package it.unipd.dei.ims.rum.utilities;

import it.unipd.dei.ims.datastructure.DatabaseState;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;


public class BlancoUsefuMethods {

	/** Given a Blaegraph statment, this method extrapolates the desired words
	 * and inserts them inside a list.
	 * */
	public static void addWordsFromStatementToList(Statement stmt, List<String> L) {

		//prendo i tre elementi della tripla
		URI subject = (URI) stmt.getSubject();
		URI predicate = (URI) stmt.getPredicate(); 
		Value object = stmt.getObject();
		
		addWordsFromBlazegraphValueToList(subject, L);
		addWordsFromBlazegraphValueToList(predicate, L);
		addWordsFromBlazegraphValueToList(object, L);

	}
	
	/** Given a Blazegraph statment, this method extrapolates the last words of the URIs 
	 * and the literals appearing  and puts them in a string.
	 * */
	public static List<String> getWordsFromStatementToList(Statement stmt) {
		List<String> list = new ArrayList<String>();
		
		//get the parts of the triples
		URI subject = (URI) stmt.getSubject();
		URI predicate = (URI) stmt.getPredicate(); 
		Value object = stmt.getObject();
		
		//add their words to the list
		addWordsFromBlazegraphValueToList(subject, list);
		addWordsFromBlazegraphValueToList(predicate, list);
		addWordsFromBlazegraphValueToList(object, list);
		
		//return the list
		return list;
		
	}
	
	/** Given a Blazegraph URL object, this method extrapolates the final words and
	 * inserts them inside a list*/
	public static void addWordsFromURIToList(URI url, List<String> L) {
		String urlString = url.stringValue();
		if(UrlUtilities.checkIfValidURL(urlString)) {
			L.add(UrlUtilities.takeWordsFromIri(urlString));
		} else {
			L.add(urlString);
		}
	}
	
	private static void addWordsFromBlazegraphValueToList(Value v, List<String> L) {
		String s = v.stringValue();
		if(UrlUtilities.checkIfValidURL(s)) {
			String st = UrlUtilities.takeWordsFromIri(s);
			if(!L.contains(st))
				L.add(st);
		} else {
			if(!L.contains(s))
				L.add(s);
		}
	}
	
	/** Add to a queue of Integer the IDs of the triples of the neighbourhood of a statement t
	 * with id id_ while exploring the query_graph E.
	 * 
	 * @param connection the jdbc Connection to the supporting database with the query_graph table representing the 
	 * graph E
	 * @param id_ the id of the triple whose neighborhood we need to explore
	 * @throws SQLException */
	public static void addNeighboursToQueue(Queue<Integer> queue, Statement t, Connection connection, int id_) throws SQLException {
		//get the subject and the object of the triple
		Value subject = t.getSubject();
		Value object = t.getObject();
		
		String sql = "select id_ from " + DatabaseState.getSchema() + ".query_graph where subject_=?";

		//SUBJECT
		//get the triples around the subject
		PreparedStatement stmt = connection.prepareStatement(sql);
		stmt.setString(1, subject.toString());
		
		ResultSet rs = stmt.executeQuery();
		while(rs.next()) {
			//get the id of the triple in the neighborhood
			int id = rs.getInt(1);
			if(id != id_ && id > id_ && !queue.contains(id)) {
				//if the triple is different from the seed triple and its ID is greater (in order to guarantee different graphs)
				queue.add(id);
			}
		}
		
		//OBJECT
		//do the same with the object if it is a URI
		if(object instanceof URI) {
			stmt = connection.prepareStatement(sql);
			stmt.setString(1, object.toString());
			
			rs = stmt.executeQuery();
			while(rs.next()) {
				//get the id of the triple
				int id = rs.getInt(1);
				if(id != id_ && id > id_ && !queue.contains(id)) {
					//if the triple is different from the seed triple and its ID is greater (in order to guarantee different graphs)
					queue.add(id);
				}
			}
		}
		
	}
	
	public static boolean stoppingCondition(List<String> bigList, List<String> smallList) {
		return bigList.containsAll(smallList);
	}
	
	/** Implements the condition described by Blanco and Elbassuoni to add a triple to 
	 * an expanding subgraph.
	 * <p>
	 * returns true if the triple can be added to the graph.
	 * 
	 * @param tripleWordList a list containing all the words of a triple
	 * */
	public static boolean addTripleCondition(List<String> tripleKeywordsList, List<List<String>> keywordsSets) {
		for(List<String> l : keywordsSets) {
			//l is a list of keywords. If the tripleKeywordsList, the list of keywords of the triple
			//is equal to l, then we must not add the triple to the subgraph
			if(l.containsAll(tripleKeywordsList) && tripleKeywordsList.containsAll(l))
				//the two lists all equal
				return false;
		}
		
		return true;
	}
	
	
	/** Given a list of words, first this method indexes the list, then extrapolates the keywords of the second
	 * list contained in the first one.
	 * 
	 * */
	public static List<String> extrapolateKeywordsFormList(List<String> list, List<String> keywordList) {
		//transform the list in a single string
		String tripleWordString = "";
		for(String s : list) {
			tripleWordString = tripleWordString + " " + s;
		}
		tripleWordString = tripleWordString.trim();
		
		//index the string and save the words in a list
		List<String> elaboratedList = TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(tripleWordString);
		
		//list with the keywords contained in the first list
		List<String> l = new ArrayList<String>();
		for(String w : elaboratedList) {
			if(keywordList.contains(w)) {
				l.add(w);
			}
		}
		
		return l;
	}
	
	
	/** This method implements a relazed version of the Blanco condition that rules the add of a new triple.
	 * <p>
	 * Returns true if the triple containing the words provided in the tripleWordsList parameter 
	 * can be added (i.e. it contains at least one new word).
	 * */
	public static boolean addTripleConditionRelaxed(List<String> tripleWordsList, Set<String> subgraphWords,
			List<String> tripleKeywordsList, List<List<String>> keywordsSets) {
		
		//first opportunity: if the new triple contains at least one new word, add it to the subgraph
		if(!subgraphWords.containsAll(tripleWordsList)) {
			return true;
		}
		
		/*second and last opportunity: if the new triple contains a combination of keywords which is
		 different from all of the others triples in the graph, we can add it anyway*/
		boolean lastChance = true;
		for(List<String> l : keywordsSets) {
			if(l.containsAll(tripleKeywordsList) && tripleKeywordsList.containsAll(l)) {
				//the two lists all equal
				lastChance = false;
				break;
			}
		}
		
		//if true, we have found a new combination. Otherwise, we have lost our last chance to add this triple
		return lastChance;
		
	}
	
	
	
}
