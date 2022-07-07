package it.unipd.dei.ims.tsa.online;

import it.unipd.dei.ims.rum.utilities.UsefulConstants;
import it.unipd.dei.ims.terrier.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.terrier.utilities.TerrierUsefulMethods;
import it.unipd.dei.ims.tsa.offline.FromRDFGraphsToTRECDocuments;
import it.unipd.dei.ims.tsa.offline.IndexerDirectoryOfTRECFiles;
import org.apache.commons.io.FileUtils;
import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.querying.SearchRequest;
import org.terrier.structures.*;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.utility.ApplicationSetup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** TSA Algorithm: phase 6
 * <p>
 * In this phase we use the query to create a first ranking of the subgraphs.
 * */
public class AnswerRankingPhase {

	/** Directory where to save the result file .res*/
	private String resultDirectory;

	/** Path of the directory where the Terrier index of
	 * the cluster documents converted in TSA is stored.
	 * */
	private String indexPath;

	private String query;

	private String queryId;

	/** Path of the directory where the representative collection
	 * is stored (as subgraphs, i.e. file ttl) 
	 * */
	private String graphClustersDir;

	/** Path of the directory where we print the graphs
	 * produced by the TSA that contain all the query
	 * words
	 * */
	private String filteredGraphsDirectory;


	/** Path of the directory where the TSA versions of the graphs
	 * that contain all the query words are put.
	 * */
	private String filteredCollectionDirectory;

	/**Path of the directory where we 
	 * build the index of the graphs that contain all the query words*/
	private String filteredCollectionIndexDirectory;

	/** Name we are going to give to the answer run file.
	 * */
	private String answerFileName;

	public AnswerRankingPhase() {
		try {
			this.setup();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public AnswerRankingPhase(boolean setup) {
		try {
			if(setup)
				this.setup();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void setup() throws IOException {
		Map<String, String> map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");

		//get the terrier home and etc directory where we have the property file and set them
		String terrierHome = map.get("terrier.home");
		String terrierEtc = map.get("terrier.etc");

		System.setProperty("terrier.home", terrierHome);
		System.setProperty("terrier.etc", terrierEtc);

		String length = map.get("answer.list.length");

		//set the number of results we want - we put a magic number here 
		System.setProperty("trec.output.format.length", length);
		System.setProperty("matching.retrieved_set_size", length);

		this.resultDirectory = map.get("result.directory");

		this.indexPath = map.get("index.path");

		query = map.get("query");

		queryId = "query_id";

		//give a default name
		this.answerFileName = "run.txt";
	}

	public void subgraphsRankingPhase() throws IOException {

		//open the index
		Index index = null;
		try {
			index = IndexOnDisk.createIndex(indexPath, "data_1");
			System.out.println("We have indexed " + index.getCollectionStatistics().getNumberOfDocuments() + " documents");
		} catch (NullPointerException e) {
			return;
		}

		Manager queryingManager = new Manager(index);

		//execute the query
		SearchRequest srq = queryingManager.newSearchRequestFromQuery(query);
		srq.addMatchingModel("Matching" , "BM25");
		queryingManager.runSearchRequest(srq);
		ResultSet results = srq.getResultSet();

		System.out.println(results.getExactResultSize()+" documents were scored");
		System.out.println("The top "+results.getResultSize()+" of those documents were returned");

		//make sure the directory where we are writing exists
		File f = new File(resultDirectory);
		if(!f.exists()) {
			f.mkdirs();
		}
		// Print the results
		Path outputPath = Paths.get(resultDirectory + "/" + this.answerFileName);
		BufferedWriter writer = Files.newBufferedWriter(outputPath, UsefulConstants.CHARSET_ENCODING);

		for (int i =0; i< results.getResultSize(); i++) {
			int docid = results.getDocids()[i];
			//identifier of the document
			String docno = index.getMetaIndex().getItem("docno", docid);
			double score = results.getScores()[i];
			//write on Standard output: (but also not, it gets quite long)
			//			System.out.println("   Rank "+i+" docid: "+ docno + " "+" "+score);
			//write on file:
			writer.write(queryId + " Q0 " + docno + " " + i + " " + score + " BM25");
			writer.newLine();
		}

		writer.close();

	}


	/** This method was added considering the characteristics
	 * of the LUBM dataset. 
	 * <p>
	 * Here we look to the whole collection of files composing the
	 * output of the TSA algorithm on the collection. Of these documents, 
	 * we only keep the ones that contain all the query words.
	 * <p>
	 * In this context, we use the definition of 'full graph'
	 * talking about a graph that contains all the keywords.
	 * */
	public void rankTheGraphsWithOnlyFullGraphs() {

		try {
			this.filterTheGraphsEfficiently();
		} catch (IOException e1) {
			System.out.println("problems filtering the graphswith query " + this.queryId);
			e1.printStackTrace();
		}

		//if we are here, we have written all the graphs containing all 
		//the query words in the new directory 

		//we convert them in TREC docs
		FromRDFGraphsToTRECDocuments converter = new FromRDFGraphsToTRECDocuments();
		converter.setGraphsMainDirectory(filteredGraphsDirectory);
		converter.setOutputDirectory(this.filteredCollectionDirectory);
		converter.convertRDFGraphsInTRECDocuments();

		//now we index them, so we can perform later BM25 on them
		IndexerDirectoryOfTRECFiles indexer = new IndexerDirectoryOfTRECFiles();
		indexer.setDirectoryToIndex(this.filteredCollectionDirectory);
		indexer.setIndexPath(this.filteredCollectionIndexDirectory);
		try {
			indexer.index("unigram");
		} catch(NullPointerException | IOException e ) {
			System.err.println("found no answering graphs for query " + queryId);
			throw new NullPointerException("found no answering graphs for query " + queryId);
		}
		
		//now we rank them with BM25
		this.setIndexPath(this.filteredCollectionIndexDirectory);
		try {
			this.subgraphsRankingPhase();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** This is a first implementation that filters the graphs that
	 * contains all the query words. I implemented it in an inefficient way,
	 * using the direct index because I could not find the correct API 
	 * (excuses, excuses...).
	 * <p>
	 * The rationale for this implementation is that the method {@link filterTheGraphsEfficiently}
	 * uses lists of documents that contains a query word. If these lists become huge,
	 * I could have a memory problem with very big databases. 
	 * Now, this method will work always since it deals with small lists, 
	 * but it uses a lot of time, so anyway it could become unfeasible.
	 * */
	@Deprecated
	private void filterTheGraphs() {

		File f = new File(this.filteredGraphsDirectory);
		f.mkdirs();

		//take the list of query words. Make sure that we are using the simple EnglishTokeniser
		ApplicationSetup.setProperty("tokeniser", "EnglishTokeniser");
		System.setProperty("tokeniser", "EnglishTokeniser");
		List<String> queryWords = TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(this.query);

		//open the index with the information about the representative collection
		Index index = IndexOnDisk.createIndex(indexPath, "data");
		
		//now we take document by document and we see what therms are in each doc
		//to do this, we need some things
		//direct index
		PostingIndex<Pointer> di = (PostingIndex<Pointer>) index.getDirectIndex();
		//document index
		DocumentIndex doi = index.getDocumentIndex();
		//lexicon of the index
		Lexicon<String> lex = index.getLexicon();
		//total number of documents inside the collection
		int docsNumber = doi.getNumberOfDocuments();
		//the index containing metadata
		MetaIndex meta = index.getMetaIndex();

		//now we can proceed
		//for each document in the collection, we take the postings representing its words
		for(int docid = 0; docid < docsNumber; ++docid) {
			try {
				IterablePosting postings = di.getPostings(doi.getDocumentEntry(docid));
				
				List<String> documentWords = new ArrayList<String>();

				//now we take all the words in the document
				while (postings.next() != IterablePosting.EOL) {
					Entry<String, LexiconEntry> lee = lex.getLexiconEntry(postings.getId());
					String words = lee.getKey();
					
					documentWords.add(lee.getKey());
				}
				
				if(docid%10000==0)
					System.out.println("checked " + docid + " graphs out of " + docsNumber);

				//if we are here, we have read all the document. Now we can check if it contains all the 
				//query words
				if(documentWords.containsAll(queryWords)) {
					//if it contains all the query words, we keep the document
					//we take the docno of the document
					String docno = meta.getItem("docno", docid);
					//we build its path
					String graphPath = this.buildGraphPath(docno);
					//copy the graph in a new directory
					File origin = new File(graphPath);
					File destination = new File(this.filteredGraphsDirectory + "/" + docno + ".ttl");
					FileUtils.copyFile(origin, destination);
				}


			} catch (IOException e) {
				System.err.print("IOException reading the terrier document " + docid + " from terrier index");
				e.printStackTrace();
			}
		}

	}

	
	private void filterTheGraphsEfficiently() throws IOException {

		File f = new File(this.filteredGraphsDirectory);
		f.mkdirs();

		//take the list of query words. Make sure that we are using the simple EnglishTokeniser
		ApplicationSetup.setProperty("tokeniser", "EnglishTokeniser");
		System.setProperty("tokeniser", "EnglishTokeniser");
		List<String> queryWords = TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(this.query);

		//open the index with the information about the representative collection
		Index index = IndexOnDisk.createIndex(indexPath, "data_1");
		System.out.println(indexPath.toString());
		
		//get the inverted index, so we have for every word the documents that contain it
		PostingIndex<Pointer> inv = (PostingIndex<Pointer>) index.getInvertedIndex();
		MetaIndex meta = index.getMetaIndex();
		Lexicon<String> lex = index.getLexicon();
		
		//list that will contain the ids of all the documents containing all the keywords
		List<Integer> idList = new ArrayList<Integer>();
		
		//to populate the list at the beginning start with the first word
		LexiconEntry le1 = lex.getLexiconEntry(queryWords.get(0));
		if(le1!=null) {
			IterablePosting postings = inv.getPostings((BitIndexPointer) le1);
			while (postings.next() != IterablePosting.EOL) {
				idList.add(postings.getId());
			}
		} if(le1==null)
			return;
			
		
		//now for the rest of the query words
		for(int i = 1; i< queryWords.size(); ++i) {
			//find the documents of this query word
			List<Integer> newSetDocId = new ArrayList<Integer>();
			LexiconEntry le = lex.getLexiconEntry(queryWords.get(i));
			if(le != null) {
				IterablePosting postings = inv.getPostings((BitIndexPointer) le);
				while (postings.next() != IterablePosting.EOL) {
					//for every document containing the word, we add it to the list
					newSetDocId.add(postings.getId());
				}
				//intersection of the new list with the first one
				idList.retainAll(newSetDocId);
			} else {
				return;
			}
		}
		
		//if we are here, in idList we have all the docid of the documents
		//containing all the query words
		for(Integer docId : idList) {
			//we take the docno of the document
			String docno = meta.getItem("docno", docId);
			//we build its path
			String graphPath = this.buildGraphPath(docno);
			//copy the graph in a new directory (need to do this because I want
			//to apply BM25 to this new 'filtered' collection, so I need an index on these)
			File origin = new File(graphPath);
			File destination = new File(this.filteredGraphsDirectory + "/" + docno + ".ttl");
			FileUtils.copyFile(origin, destination);
		}
	}


	private String buildGraphPath(String docno) {
		String path = this.graphClustersDir;
		int docNo = Integer.parseInt(docno);
		int offset = (int) Math.ceil((double) docNo / 2048); 


		return path + "/" + offset + "/" + docno + ".ttl";
	}

	/** Testing main
	 * */
	public static void main(String[] args) throws IOException {
		AnswerRankingPhase phase = new AnswerRankingPhase();
		phase.subgraphsRankingPhase();
	}

	public String getResultDirectory() {
		return resultDirectory;
	}

	public void setResultDirectory(String resultDirectory) {
		this.resultDirectory = resultDirectory;
	}

	public String getIndexPath() {
		return indexPath;
	}

	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getAnswerFileName() {
		return answerFileName;
	}

	public void setAnswerFileName(String answerFileName) {
		this.answerFileName = answerFileName;
	}

	public String getGraphClustersDir() {
		return graphClustersDir;
	}

	public void setGraphClustersDir(String graphClustersDir) {
		this.graphClustersDir = graphClustersDir;
	}

	public String getFilteredGraphsDirectory() {
		return filteredGraphsDirectory;
	}

	public void setFilteredGraphsDirectory(String filteredGraphsDirectory) {
		this.filteredGraphsDirectory = filteredGraphsDirectory;
	}

	public String getFilteredCollectionDirectory() {
		return filteredCollectionDirectory;
	}

	public void setFilteredCollectionDirectory(String filteredCollectionDirectory) {
		this.filteredCollectionDirectory = filteredCollectionDirectory;
	}

	public String getFilteredCollectionIndexDirectory() {
		return filteredCollectionIndexDirectory;
	}

	public void setFilteredCollectionIndexDirectory(String filteredGraphsIndexDirectory) {
		this.filteredCollectionIndexDirectory = filteredGraphsIndexDirectory;
	}

}
