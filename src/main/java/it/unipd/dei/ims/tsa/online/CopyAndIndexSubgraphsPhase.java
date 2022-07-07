package it.unipd.dei.ims.tsa.online;

import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.tsa.offline.FromRDFGraphsToTRECDocuments;
import it.unipd.dei.ims.tsa.offline.IndexerDirectoryOfTRECFiles;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/** TSA algorithm: phase 7
 * <p>
 * We use the answer generated using a standard IR method in phase 6
 * to create a directory with the first answers graph and their index.
 * 
 * */
public class CopyAndIndexSubgraphsPhase {

	/** Path of the directory where we are working.
	 * */
	private String queryDirectoryPath;

	/** Path of the file .res
	 * */
	private String resourceFilePath;

	/** Directory where the aggregated graphs are stored
	 * */
	private String rdfGraphsDirectory;

	/** Path of the directory where to save the collection
	 * made of graphs.
	 * */
	private String outputCandidateGraphsDirectoryPath;

	/** Path of the directory where to save the collection
	 * made of TREC documents.
	 * */
	private String outputCandidateCollectionDirectoryPath;

	/** path of the directory where to save the index derived
	 * from the candidate collection
	 * */
	private String outputCollectionIndexPath;

	private boolean greenLight;

	public CopyAndIndexSubgraphsPhase() {
		greenLight = true;
		this.setup();
	}

	private void setup() {
		try {
			Map<String, String> map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");

			this.queryDirectoryPath = map.get("query.directory.path");
			this.resourceFilePath =  map.get("resource.file.path");
			this.rdfGraphsDirectory = map.get("rdf.graphs.directory");
			this.outputCandidateGraphsDirectoryPath = map.get("output.candidate.graphs.directory.path");
			this.outputCandidateCollectionDirectoryPath = map.get("output.candidate.collection.directory.path");
			this.outputCollectionIndexPath = map.get("output.collection.index.path");

		} catch (IOException e) {
			System.err.println("Error reading the properties, ABORT!");
			e.printStackTrace();
			greenLight = false;
		}
	}

	/** Copies the candidate answer graphs, converts them in TREC files and
	 * indexes them.
	 * */
	public void createCandidateCollection () throws IOException {
		this.createGraphCandidateCollection();
		this.createTRECCandidateCollection();
		this.indexCandidateCollection();
	}

	/** This method uses the file .res contained in the directory
	 * [query.directory.path]/IR/TSA+IR.res 
	 * to find out the graphs that have been 
	 * retrieved with IR, and creates a new colletion, called Candidate Collection.
	 * */
	private void createGraphCandidateCollection () {
		//clean the directory
		File f = new File(this.outputCandidateGraphsDirectoryPath);
		if(!f.exists()) 
			f.mkdirs();
		try {
			FileUtils.cleanDirectory(f);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		//read the file
		Path input = Paths.get(this.resourceFilePath);
		int counter = 0;
		try(BufferedReader reader = Files.newBufferedReader(input)) {
			String line = "";
			while((line = reader.readLine()) != null) {
				//for each candidate graph
				String[] lineParts = line.split(" ");
				//the id of the document is the third component in the line
				int id = Integer.parseInt(lineParts[2]);
				counter = id;
				//hash function to find the path of the file
				int dirId = (int) Math.ceil((double) id / 2048);
				//build the path of the source graph to copy 
				String graphPath = this.rdfGraphsDirectory + "/" + dirId + "/" + id + ".ttl";
				Path source = Paths.get(graphPath);

				//build the output
				File out = new File(this.outputCandidateGraphsDirectoryPath + "/" + id + ".ttl");
				OutputStream outStream = new FileOutputStream(out);
				//copy the graph in the new directory
				Files.copy(source, outStream);
				outStream.close();

			}
		} catch (IOException e) {
			System.err.println("Error reading path " + input + " at id: " + counter);
			e.printStackTrace();
		}
	}

	/** Converts the directory of RDF graphs in a directory of TREC documents
	 * */
	private void createTRECCandidateCollection () {
		//clean the directory
		File f = new File(this.outputCandidateCollectionDirectoryPath);
		if(!f.exists()) 
			f.mkdirs();
		try {
			FileUtils.cleanDirectory(f);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		FromRDFGraphsToTRECDocuments converter = new FromRDFGraphsToTRECDocuments ();
		//set the input and output directory
		converter.setGraphsMainDirectory(this.outputCandidateGraphsDirectoryPath);
		converter.setOutputDirectory(this.outputCandidateCollectionDirectoryPath);

		//execute the conversion
		converter.convertRDFGraphsInTRECDocuments();
	}

	private void indexCandidateCollection() throws IOException {
		//clean the directory
		File f = new File(this.outputCollectionIndexPath);
		if(!f.exists()) 
			f.mkdirs();
		try {
			FileUtils.cleanDirectory(f);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		IndexerDirectoryOfTRECFiles indexer = new IndexerDirectoryOfTRECFiles();
		indexer.setDirectoryToIndex(this.outputCandidateCollectionDirectoryPath);
		indexer.setIndexPath(this.outputCollectionIndexPath);

		indexer.index("unigram");
	}

	public String getQueryDirectoryPath() {
		return queryDirectoryPath;
	}

	public void setQueryDirectoryPath(String queryDirectoryPath) {
		this.queryDirectoryPath = queryDirectoryPath;
	}

	public String getResourceFilePath() {
		return resourceFilePath;
	}

	public void setResourceFilePath(String resourceFilePath) {
		this.resourceFilePath = resourceFilePath;
	}

	public String getRdfGraphsDirectory() {
		return rdfGraphsDirectory;
	}

	public void setRdfGraphsDirectory(String rdfGraphsDirectory) {
		this.rdfGraphsDirectory = rdfGraphsDirectory;
	}

	public String getOutputCandidateGraphsDirectoryPath() {
		return outputCandidateGraphsDirectoryPath;
	}

	public void setOutputCandidateGraphsDirectoryPath(String outputCandidateGraphsDirectoryPath) {
		this.outputCandidateGraphsDirectoryPath = outputCandidateGraphsDirectoryPath;
	}

	public String getOutputCandidateCollectionDirectoryPath() {
		return outputCandidateCollectionDirectoryPath;
	}

	public void setOutputCandidateCollectionDirectoryPath(String outputCandidateCollectionDirectoryPath) {
		this.outputCandidateCollectionDirectoryPath = outputCandidateCollectionDirectoryPath;
	}

	public String getOutputCollectionIndexPath() {
		return outputCollectionIndexPath;
	}

	public void setOutputCollectionIndexPath(String outputCollectionIndexPath) {
		this.outputCollectionIndexPath = outputCollectionIndexPath;
	}


}
