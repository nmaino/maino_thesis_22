package it.unipd.dei.ims.rum.utilities;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import it.unipd.dei.ims.terrier.utilities.TerrierUsefulMethods;
import org.openrdf.model.*;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.StatementCollector;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**Contains useful methods when working with Blazegraph (repetitive tasks)
 * */
public class BlazegraphUsefulMethods {

	/** Creates a Blazegraph repository to the file whose path has been provided. 
	 * NB: to utilize the repository you need to .initialize() it before.
	 * When you have finished, you need to close it. 
	 * */
	public static Repository createRepository(String repositoryFile) {
		final Properties props = new Properties();
		props.put(BigdataSail.Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		props.put(BigdataSail.Options.FILE, repositoryFile);

		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository
		return repo;
	}
	
	
	public static Repository getLocalRepository() throws RepositoryException {
		String baseDirName = "./blazegraph/";
		Properties properties = new Properties();
		Path baseDir = Paths.get(baseDirName).toAbsolutePath();
		Path journalPath = baseDir.resolve("test.jnl");
		properties.put(BigdataSail.Options.BUFFER_MODE, "DiskRW");
		properties.put(BigdataSail.Options.FILE, journalPath.getFileName().toString());
		final BigdataSail sail = new BigdataSail(properties);
		final Repository repository = new BigdataSailRepository(sail);
		repository.initialize();
		return repository;
	}
	
	
	/** Creates an RDF triple/statement given the three strings composing it.
	 * */
	public static Statement createAStatement(String subject, String predicate, String object) {
		URI subj = new URIImpl(subject);
		URI pred = new URIImpl(predicate);
		Value obj;
		
		if(UrlUtilities.checkIfValidURL(object)) {
			//obj is a URL
			obj = new URIImpl(object);
		}
		else {//literal
			obj = BlazegraphUsefulMethods.dealWithTheObjectLiteralString(object);
		}
		Statement stat = new StatementImpl(subj, (URI) pred, obj);
		return stat;
	}
	
	
	/**Creates a RepositoryConnection given a repository, so you can then query the
	 * dataset contained in that repository. 
	 * <p>
	 * NB: this method opens a READ ONLY connection
	 * <p>
	 * Remember to close it at the end of utilization. 
	 * */
	public static RepositoryConnection getRepositoryConnection(Repository repo) {

		RepositoryConnection cxn = null;

		try {
			// open connection
			if (repo instanceof BigdataSailRepository) {
				cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
			} else {
				cxn = repo.getConnection();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		return cxn;
	}

	/** Creates an iterator other the triples of a database. It performs the SPARQL query:
	 * <p>
	 * select ?s ?p ?o where { ?s ?p ?o . }
	 * 
	 * NB: remember to close the TupleQueryResult when done.
	 * */
	public static TupleQueryResult getIterator(RepositoryConnection cxn) {
		TupleQuery tupleQuery;
		try {
			tupleQuery = cxn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							"select ?s ?p ?o where { ?s ?p ?o . }");
			TupleQueryResult result = tupleQuery.evaluate();

			return result;
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**Return a statement iterator other all the triples of the database.
	 * */
	public static RepositoryResult<Statement> getIteratorOfStatements(RepositoryConnection conn) 
			throws RepositoryException {

		RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true);
		//		Statement s = statements.next();
		return statements;
	}

	public static int getDegreeOfATriple2(RepositoryConnection conn, BindingSet bs) 
			throws RepositoryException {
		int degree = 0; 
		
		Value s = bs.getValue("s");
		Resource subj = new URIImpl(s.toString());
		
		Value o = bs.getValue("o");

		RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, false);
		while(statements.hasNext()) {
			degree++;
			statements.next();
		}
		statements = conn.getStatements(null, null, o, false);
		while(statements.hasNext()) {
			degree++;
			statements.next();
		}
		statements.close();
		return degree-2;
	}

	/**Given a connection to a database and a BindingSet representing a triple,
	 * returns the degree of the triple.
	 * 
	 * NB: this methods requires a lot of time, therefore it is not 
	 * usable in an application with millions of triples.
	 * */
	public static int getDegreeOfATriple(RepositoryConnection conn, BindingSet bs) throws RepositoryException {
		int degree = 0; 
		TupleQuery tupleQuery;
		Value s = bs.getValue("s");
		Value o = bs.getValue("o");
		try {
			tupleQuery = conn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							"select ?s ?p ?o where { <" + s.toString() + "> ?p ?o . }");
			TupleQueryResult result = tupleQuery.evaluate();
			while(result.hasNext()) {
				result.next();
				degree++;
			}

			
			//check that the object is a url or not (the objects can be literal)
			String query = null;
			if(o instanceof URI) {
				query = "select ?s ?p ?o where { ?s ?p <" + o.toString() + "> . }";
			}
			else if (o instanceof Literal) {
				query = "select ?s ?p ?o where { ?s ?p " + o.toString() + " . }";
			}
			
			tupleQuery = conn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							query);
			result = tupleQuery.evaluate();
			while(result.hasNext()) {
				result.next();
				degree++;
			}

			degree = degree -2;
			return degree;

			//SELECT (count(*) AS ?count) { ?s ?p ?o .}
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;

	}
	
	public static int getDegreeOfATriple(RepositoryConnection conn, Statement bs) throws RepositoryException {
		int degree = 0; 
		TupleQuery tupleQuery;
		Value s = bs.getSubject();
		Value o = bs.getObject();
		try {
			tupleQuery = conn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							"select ?s ?p ?o where { <" + s.toString() + "> ?p ?o . }");
			TupleQueryResult result = tupleQuery.evaluate();
			while(result.hasNext()) {
				result.next();
				degree++;
			}

			
			//check that the object is a url or not (the objects can be literal)
			String query = null;
			if(o instanceof URI) {
				query = "select ?s ?p ?o where { ?s ?p <" + o.toString() + "> . }";
			}
			else if (o instanceof Literal) {
				query = "select ?s ?p ?o where { ?s ?p " + o.toString() + " . }";
			}
			
			tupleQuery = conn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							query);
			result = tupleQuery.evaluate();
			while(result.hasNext()) {
				result.next();
				degree++;
			}

			degree = degree -2;
			return degree;

			//SELECT (count(*) AS ?count) { ?s ?p ?o .}
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;

	}

	/**Returns the repository corresponding to the path string. 
	 * 
	 * NB: the repository needs to be initialized before use.
	 * */
	public static Repository getRepositoryFromPath(String p) {

		Properties props = new Properties();
		props.put(Options.BUFFER_MODE, "DiskRW");
		props.put(Options.FILE, p);

		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository
		return repo;
	}

	/**Given a node identified by the string s, returns a tupleQueryResult
	 * with the triples around the node.
	 * 
	 * @param cxn RepositoryConnection to the dataset
	 * @param s String representing the node
	 * */
	public static TupleQueryResult listTriples(RepositoryConnection cxn, String s) {
		TupleQuery tupleQuery;
		try {
			tupleQuery = cxn
					.prepareTupleQuery(QueryLanguage.SPARQL,
							"select ?p ?o where { <" + s + "> ?p ?o . }");
			TupleQueryResult result = tupleQuery.evaluate();

			return result;
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return null;
	}

	/** Given a node identified by the string s, returns a list of
	 * statements with that node as subject.
	 * 
	 * NB: remember to close the RepositoryResult when it is over
	 * 
	 * @param cxn RepositoryConnection to the dataset
	 * @param s String representing the node*/
	public static RepositoryResult<Statement> listStatements(RepositoryConnection conn, String s) throws RepositoryException {
		Resource sbj = new URIImpl(s);
		RepositoryResult<Statement> statements = conn.getStatements(sbj, null, null, false);
		return statements;
	}
	
	/** Given a string representing a literal read from a text file representing 
	 * an RDF graph or another source, creates the correct Literal object in Blazegraph.
	 * */
	public static Literal dealWithTheObjectLiteralString(String obj) {
		String regex = "\"(.*)\"(\\^\\^<(.*)>)?(@(.*))?";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(obj);
		if(matcher.find()) {
			String firstString = matcher.group(1);
			String uriString = matcher.group(3);
			String languageString = matcher.group(5); 
			if(uriString == null && languageString == null) {
				//only a single string, nothing else
				Literal literal = new LiteralImpl(firstString);
				return literal;
			}
			else if (uriString!= null && languageString == null) {
				//a literal with its type after ^^
				Literal literal;
				try {
					URI uri = new URIImpl(uriString);
					literal = new LiteralImpl(firstString, uri);
				}
				catch(Exception e) {
					System.err.println("Strange url: " + uriString);
					literal = new LiteralImpl(firstString+uriString);
				}
				return literal;
			}
			else if (uriString == null && languageString != null) {
				Literal literal = new LiteralImpl(firstString, languageString);
				return literal;
			}
		} else {
			Literal literal = new LiteralImpl(obj);
			return literal;
		}
		return null;
	}
	
	/** Ridenomination of {@link dealWithTheObjectLiteralString} because it is simpler
	 * to remember.
	 * */
	public static Literal dealWithALiteral(String obj) {
		return dealWithTheObjectLiteralString(obj);
	}
	
	
	/** Given a Blazegraph Model, it prints it in Turtle syntax in the 
	 * specified file path in turtle syntax.
	 * 
	 * */
	public static void printTheDamnGraph(Model graph, String path) {
		File f = new File(path);
		try(OutputStream out = new FileOutputStream(f)) {
			RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, out);
			writer.startRDF();
			for(Statement st : graph) {
				writer.handleStatement(st);
			}
			writer.endRDF();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}
	}
	
	/** Given a Blazegraph Model, it prints it in Turtle syntax in the 
	 * specified file path.
	 * 
	 * */
	public static void printTheDamnGraph(Model graph, String path, RDFFormat format) {
		File f = new File(path);
		try(OutputStream out = new FileOutputStream(f)) {
			RDFWriter writer = Rio.createWriter(format, out);
			writer.startRDF();
			for(Statement st : graph) {
				writer.handleStatement(st);
			}
			writer.endRDF();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}
	}
	
	/** Transforms a triple (statement) in a string of text. 
	 * */
	public static String fromStatementToDocument(Statement stmt) {
		List<String> list = BlancoUsefuMethods.getWordsFromStatementToList(stmt);
		String r = "";
		for(String w : list) {
			r = r + " " + w;
		}
		return r;
	}
	
	public static String fromStatementToStringWithRepetition(Statement t) {
		String r = "";
		
		String subject = t.getSubject().toString();
		r += UrlUtilities.takeWordsFromIri(subject) + " ";
		String predicate = t.getPredicate().toString();
		r += UrlUtilities.takeWordsFromIri(predicate) + " ";
		String object = t.getObject().stringValue();
		if(UrlUtilities.checkIfValidURL(object)) {
			r += UrlUtilities.takeWordsFromIri(object) + " ";
		} else {
			r += object + " ";
		}
		
		return r;
		
	}
	
	/** Transforms a whole RDF graph in memory in a document string.
	 * */
	public static String fromGraphToDocument(Model g) {
		String document = "";
		for(Statement t : g) {
			document += BlazegraphUsefulMethods.fromStatementToStringWithRepetition(t) + " ";
		}
		return document;
	}
	
	/** Given a Blazegraph statment, this method extrapolates the last words of the URIs 
	 * and the literals appearing  and puts them in a list.
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
	
	/** Reads one graph from a text file in turtle format
	 * */
	public static Collection<Statement> readOneGraph(String path) throws RDFParseException, RDFHandlerException {
		//open the input stream to the file
		try(InputStream inputStream = new FileInputStream(new File(path));) {
			//prepare a collector to contain the triples
			StatementCollector collector = new StatementCollector();
			//read the file
			RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
			//link the collector to the parser
			rdfParser.setRDFHandler(collector);
			//parse the file
			rdfParser.parse(inputStream, "");
			//now get the triples/statements composing the graph
			Collection<Statement> statements = collector.getStatements();
			
			return statements;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	/** Reads one graph from a text file in turtle format
	 * */
	public static Collection<Statement> readOneGraph(String path, RDFFormat format) throws RDFParseException, RDFHandlerException {
		//open the input stream to the file
		try(InputStream inputStream = new FileInputStream(new File(path));) {
			//prepare a collector to contain the triples
			StatementCollector collector = new StatementCollector();
			//read the file
			RDFParser rdfParser = Rio.createParser(format);
			//link the collector to the parser
			rdfParser.setRDFHandler(collector);
			//parse the file
			rdfParser.parse(inputStream, "");
			//now get the triples/statements composing the graph
			Collection<Statement> statements = collector.getStatements();
			
			return statements;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**The same method as {@link readOneGraph}, but it deals with the 
	 * exceptions internally. If necessary, use the other method
	 * to work with the answers.
	 * */
	public static Collection<Statement> readGraph(String path) {
		try {
			return BlazegraphUsefulMethods.readOneGraph(path);
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/** Checks if a triple contains a query words
	 * */
	public static boolean containsQueryWord(Statement t, List<String> queryWords) {
		String doc = BlazegraphUsefulMethods.fromStatementToDocument(t);
		List<String> tripleWords = TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(doc);
		tripleWords.retainAll(queryWords);
		if(tripleWords.size() > 0)
			return true;
		else
			return false;
	}
	
	/** Returns true if the prodived Set contains a triple
	 * with the same subject, predicate and object of the triple t.
	 * 
	 * */
	public static boolean ifListContainsTriple(Set<Statement> list, Statement t) {
		String tSbj = t.getSubject().stringValue();
		String tPrd = t.getPredicate().stringValue();
		String tObj = t.getObject().stringValue();
		
		for(Statement lt : list) {
			String ltSbj = lt.getSubject().stringValue();
			String ltPrd = lt.getPredicate().stringValue();
			String ltObj = lt.getObject().stringValue();
			
			if(ltSbj.equals(tSbj) && ltPrd.equals(tPrd) && ltObj.equals(tObj)) {
				//triple found
				return true;
			}
		}//end for
		
		//this triple was not found
		return false;
	}
	
	/** Given a node and the graph model containing it, returns the corresponsing meta node.
	 * That is, the model made up of that node, its literal neighbou and the 'type'
	 * attributes.
	 * <p>
	 * NB: since I found that certain keywords may appear only in the predicate and not in the node,
	 * it is necessary to include in the neighbourhood also the triples with 
	 * object an URL (for example, in IMDB the 'genre' keyword only appears in the predicate) 
	 * */
	public static Model returnMetaNodeWithCenterThis(Resource subject, Model m) {
		Model ret = new TreeModel();
		Model neighbour = m.filter(subject, null, null);
		for(Statement n : neighbour) {
			Value o = n.getObject();
			URI p = n.getPredicate();
//			if(o instanceof Literal) {
//				//we keep it
//				ret.add(n);
//			} 
//			
//			String val = UrlUtilities.takeFinalWordFromIRI(p.toString());
//			if(val.contains("type")) {
//				//it means we are dealing with a type statement
//				ret.add(n);
//			}
			
			//easy fix: we add the neighbour anyway to include
			//as many keywords as possible
			ret.add(n);
		}
		
		
		
		return ret;
	}

}
