package it.unipd.dei.ims.tsa.offline;

import it.unipd.dei.ims.rum.utilities.PathUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import org.terrier.indexing.Collection;
import org.terrier.indexing.TRECCollection;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.indexing.classical.BasicIndexer;
import org.terrier.utility.ApplicationSetup;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** TSA Algorithm: phase 5.
 * <p>
 * Creation of the index of the answer graphs. This is a more generic class that can be used to 
 * index documents of a collection
 * */
public class IndexerDirectoryOfTRECFiles {

	/** The directory containing the files that you need to index.
	 * <p>
	 * property: directory.to.index.
	 * */
	private String directoryToIndex;

	private String indexPath;

	private String terrierHome;
	private String terrierEtc;

	public IndexerDirectoryOfTRECFiles() {

		try {
			this.setup();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public IndexerDirectoryOfTRECFiles(boolean setup) {

		if(setup) {
			try {
				this.setup();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}


	private void setup() throws IOException {

		Map<String, String> map = 
				PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");
		this.directoryToIndex = map.get("directory.to.index");
		this.indexPath = map.get("index.path");

		this.terrierHome = map.get("terrier.home");
		this.terrierEtc = map.get("terrier.etc");
		//set the needed properties
		
		System.setProperty("terrier.home", terrierHome);
		System.setProperty("terrier.etc", terrierEtc);
	}

	/** Executes the TREC documents contained in the 
	 * directory field. 
	 * 
	 * @param flag set to 'unigram' if you want a index of unigrams. 'bigram'
	 * if you want an index made of bigrams.
	 * 
	 * */
	public void index(String flag) {
		//set what kind of indexing we want
		if(flag.equals("unigram"))
			System.setProperty("tokeniser", "EnglishTokeniser"); 
		else if(flag.equals("bigram")) {
			ApplicationSetup.setProperty("tokeniser", "BigramTokeniser");
			System.setProperty("tokeniser", "BigramTokeniser");
		}

		//checkout that the directory exists or build it 
		File f = new File(indexPath);
		if(!f.exists()) {
			f.mkdirs();
		}

		//get the files that we want to index
		List<String> files = PathUsefulMethods.getListOfFiles(directoryToIndex);
		//indexer with the path of the directory where to index
		BasicIndexer indexer = new BasicIndexer(indexPath, "data_1");
		Collection coll = new TRECCollection(files);

		indexer.index(new Collection[]{ coll });
		//open the new index
		Index index = IndexOnDisk.createIndex(indexPath, "data_1");
		try {
			index.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			//re-set the standard tokenizer 
			System.setProperty("tokeniser", "EnglishTokeniser"); 
		}

		System.out.println("Index created.");
	}


	/** Test main*/
	public static void main(String[] args) {
		IndexerDirectoryOfTRECFiles phase = new IndexerDirectoryOfTRECFiles();
		
		phase.setDirectoryToIndex("/Users/Nicola Maino/Documents/RDF_DATASETS/linkedmdb-latest-dump/algorithms/TSA/TREC");
		phase.setIndexPath(".");
		phase.index("unigram");
	}


	public String getDirectoryToIndex() {
		return directoryToIndex;
	}


	public void setDirectoryToIndex(String directoryToIndex) {
		this.directoryToIndex = directoryToIndex;
	}


	public String getIndexPath() {
		return indexPath;
	}


	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

}
