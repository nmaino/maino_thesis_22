package it.unipd.dei.ims.tsa.online;

import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.terrier.utilities.TerrierUsefulMethods;
import it.unipd.dei.ims.tsa.offline.FromRDFGraphsToTRECDocuments;
import it.unipd.dei.ims.tsa.offline.IndexerDirectoryOfTRECFiles;
import org.apache.commons.io.FileUtils;
import org.terrier.structures.*;
import org.terrier.structures.postings.IterablePosting;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** NEW PHASE 2 
 * At the end of phase 6 and 7, so maybe phase 7.2?
 * <p>
 * Uses the unigram index of the merged collection to find only the graphs 
 * that contain all the query words.
 * <p>
 * This has proved in poorer results from IMDB, but an increase
 * for LinkedMDB, so maybe, but it's not determinant. For now, it is suspended.
 * 
 * */
public class FindBestGraphsPhase {

	/** The query we are working on.
	 * */
	private String query;

	/** Path of the directory where the index of the collection is stored
	 * */
	private String collectionIndexDirectory;
	
	/** Directory with the graph files of the original collection*/
	private String collectionGraphDirectory;

	/** where to save the collection of virtual documents*/
	private String newCollectionPath;

	private String terrierHome, terrierEtc;


	/**property map*/
	private Map<String, String> map;

	public FindBestGraphsPhase() {
		try {
			this.map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");
			this.query = map.get("query");
			this.collectionIndexDirectory = map.get("collection.index.directory");

			this.terrierHome = map.get("terrier.home");
			this.terrierEtc = map.get("terrier.etc");
			//set the needed properties

			System.setProperty("terrier.home", terrierHome);
			System.setProperty("terrier.etc", terrierEtc);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> findBestGraphs() throws IOException {
		//open the index
		Index index = IndexOnDisk.createIndex(this.collectionIndexDirectory, "data_1");
		System.out.printf("index created\n");

		//get the wuery terms filtered with terrier
		List<String> queryWords = TerrierUsefulMethods.getDocumentWordsWithTerrierAsList(query);

		//find the query words in the documents
		PostingIndex<Pointer> invertedIndex = (PostingIndex<Pointer>) index.getInvertedIndex();
		MetaIndex meta = index.getMetaIndex();
		Lexicon<String> lex = index.getLexicon();

		//list with the docid of the documents with the query words
		List<Integer> candidatesList = new ArrayList<Integer>();

		//get the lexicon entry corresponding to the first query word
		LexiconEntry le = lex.getLexiconEntry( queryWords.get(0) );
		if(le!=null) {
			//get the documents containing the query word
			IterablePosting postings = invertedIndex.getPostings((BitIndexPointer) le);
			//find the documents containing this word
			while (postings.next() != IterablePosting.EOL) {
				//				String docno = meta.getItem("docno", postings.getId());
				candidatesList.add(postings.getId());
			}
		}

		//now we proceed with the other query words, performing the intersection
		//of the sets

		for(int i = 1; i< queryWords.size(); ++i) {
			//find the documents of this query word
			List<Integer> newSet = new ArrayList<Integer>();
			//TODO: verify le2
			LexiconEntry le2 = lex.getLexiconEntry(queryWords.get(i));
			if(le != null) {
				IterablePosting postings = invertedIndex.getPostings((BitIndexPointer) le);
				while (postings.next() != IterablePosting.EOL) {
					newSet.add(postings.getId());
				}
				//intersection of the new list with the first one
				candidatesList.retainAll(newSet);
			}
		}

		List<String> docNoList = new ArrayList<String>();
		for(int i = 0; i <  candidatesList.size(); ++i) {
			int docid = candidatesList.get(i);
			String docno = meta.getItem("docno", docid);
			docNoList.add(docno);
		}
		System.out.println("candidate virtual documents: " + docNoList + "\n" + docNoList.size());

		return docNoList;
	}

	public void createNewCollectionCopyingFromListOfDocno(List<String> docNoList) throws IOException {
		//create the directory where to write the graphs, the corresponding trec collection and the index
		File f = new File(this.newCollectionPath);
		if(!f.exists()) {
			f.mkdirs();
		}
		String newCollectionDirectory  = f.getParentFile().getAbsolutePath() + "/ultimate_collection";
		File g = new File(newCollectionDirectory);
		if(!g.exists()) {
			g.mkdirs();
		}
		String newIndexDirectory = f.getParentFile().getAbsolutePath() + "/ultimate_index";
		File x = new File(newIndexDirectory);
		if(!x.exists()) {
			x.mkdirs();
		}

		for(String docNo : docNoList) {
			//source file
			String p = this.collectionGraphDirectory + "/" + docNo + ".ttl";
			File srcFile = new File(p);
			//destination file
			p = this.newCollectionPath + "/" + docNo + ".ttl";
			File destFile = new File(p);

			FileUtils.copyFile(srcFile, destFile);
		}

		//now convert the files in trec documents
		//convert the new collection of graphs in documents and index them
		FromRDFGraphsToTRECDocuments converter = new FromRDFGraphsToTRECDocuments();
		converter.setGraphsMainDirectory(this.newCollectionPath); 
		converter.setOutputDirectory(newCollectionDirectory);
		converter.convertRDFGraphsInTRECDocuments();
		
		//index everything
		IndexerDirectoryOfTRECFiles indexer = new IndexerDirectoryOfTRECFiles();
		indexer.setDirectoryToIndex(newCollectionDirectory);
		indexer.setIndexPath(newIndexDirectory);
		indexer.index("unigram");
	}
	


	/** test main*/
	public static void main(String[] args) {
		FindBestGraphsPhase execution = new FindBestGraphsPhase();

		execution.setQuery("Mel Gibson director title");
		execution.setCollectionIndexDirectory(".");

		try {
			execution.findBestGraphs();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	////////// getters and setters

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getCollectionIndexDirectory() {
		return collectionIndexDirectory;
	}

	public void setCollectionIndexDirectory(String collectionIndexDirectory) {
		this.collectionIndexDirectory = collectionIndexDirectory;
	}

	public String getNewCollectionPath() {
		return newCollectionPath;
	}

	public void setNewCollectionPath(String newCollectionPath) {
		this.newCollectionPath = newCollectionPath;
	}

	public String getCollectionGraphDirectory() {
		return collectionGraphDirectory;
	}

	public void setCollectionGraphDirectory(String collectionGraphDirectory) {
		this.collectionGraphDirectory = collectionGraphDirectory;
	}


}
