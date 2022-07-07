package it.unipd.dei.ims.tsa.online;

import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.rum.utilities.UsefulMathFunctions;
import org.apache.commons.io.FileUtils;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** This class implements a simple heuristic to combine
 * graphs in an ordered list. When two adjacent graphs are overlapped for more than 
 * 30%, we fuse the two graphs in order to obtain a unique answer.
 * 
 * */
public class AnswerCompressionPhase {


	/** Path of the file .res with the rank of the graphs.
	 * */
	private String rankingFilePath;

	/** Path of the directory with the original graphs.
	 * In the case of TSA, we are talking of the directory with the clusters.*/
	private String originalCollectionDirectoryPath;

	/** Path of the directory where we are going to print the new graphs.
	 * */
	private String compressedCollectionOutputDirectoryPath;

	/** percentage of overlapping above which we 
	 * merge two graphs.
	 * */
	private double compressionThreshold;
	
	private int lookaheadThreshold;

	public AnswerCompressionPhase() {
		try {
			this.setup();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void setup() throws IOException {
		Map<String, String> map = 
				PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");

		this.rankingFilePath = map.get("ranking.file.path");
		this.originalCollectionDirectoryPath = map.get("original.collection.directory.path");
		this.compressedCollectionOutputDirectoryPath = map.get("compressed.collection.output.directory.path");
		this.compressionThreshold = Double.parseDouble(map.get("compression.threshold"));
		this.lookaheadThreshold = Integer.parseInt(map.get("lookahead.threshold"));
	}

	/** Version of the compression algorithm. For each graph, 
	 * reads the x following graphs and decide if it could be
	 * useful to merge them. THe merged graphs are
	 * removed from the list and the process continues.
	 * <p>
	 * NB: to note that the algorithm also performs a 
	 * cut-off of the collection to a max of 1000 elements as of now.
	 * */
	public void compressTheCollectionWithThreshold() {

		File f = new File(this.compressedCollectionOutputDirectoryPath);
		if(!f.exists()) {
			f.mkdirs();
		}
		try {
			FileUtils.cleanDirectory(f);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		//read the ranking file so to obtain the list of graphs
		Path rankPath = Paths.get(this.rankingFilePath);
		try(BufferedReader reader = Files.newBufferedReader(rankPath)) {
			//read line per line to get al the ids
			String line = "";
			String[] parts;
			String graphId;
			//list to keep the ids of the graphs in the original collection
			List<String> idList = new LinkedList<String>();
			//read all the ids and save them in the list
			while(( (line = reader.readLine()) != null) ){
				//get the next graph
				parts = line.split(" ");
				graphId = parts[2];
				idList.add(graphId);
			}

			int counter = 0;
			
			int mergeCounter = 0;
			
			
			
			File t = new File(this.compressedCollectionOutputDirectoryPath);
			t = t.getParentFile().getParentFile().getParentFile();
			String outp = t.getAbsolutePath() + "/merge_infos.txt";
			FileWriter fw = new FileWriter(outp, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter out = new PrintWriter(bw);
			
			out.write("information about merging with percentage " + this.compressionThreshold
					+ " and lookahead " + this.lookaheadThreshold + "\n");
			
			//compress the graphs
			while(!idList.isEmpty() && counter < 1000) {
				//get the first element of the list
				String id = idList.get(0);
				idList.remove(id);

				//read the first graph
				int offset = UsefulMathFunctions.subDirectoryIndexForClusters(Integer.parseInt(id));
				String graphPath = this.getOriginalCollectionDirectoryPath() + "/" + offset + "/" + id + ".ttl";
				Model graph = new TreeModel(BlazegraphUsefulMethods.readOneGraph(graphPath));

				List<String> delendumList = new ArrayList<String>();
				for(int i = 0; i < this.lookaheadThreshold; ++i) {
					if(idList.size() <= i)
						break;
					
					id = idList.get(i);
					//read the graph at position i
					offset = UsefulMathFunctions.subDirectoryIndexForClusters(Integer.parseInt(id));
					graphPath = this.getOriginalCollectionDirectoryPath() + "/" + offset + "/" + id + ".ttl";
					Model newGraph = new TreeModel(BlazegraphUsefulMethods.readOneGraph(graphPath));
					//decide if we should merge the two graphs
					if(decideIfCombine(graph, newGraph)) {
						mergeCounter++;
						graph.addAll(newGraph);
						//keep track of the merged graphs
						delendumList.add(id);
						newGraph.clear();
					}
				}
				//print the result
				BlazegraphUsefulMethods.printTheDamnGraph(graph, this.compressedCollectionOutputDirectoryPath + "/" + counter + ".ttl");
				counter++;
				
				//delete all the graphs that have been merged
				for(int j = 0; j < delendumList.size(); ++j) {
					idList.remove(delendumList.get(j));
				}
				delendumList.clear();

			}
			
			System.out.println("total number of performed merges brother: " + mergeCounter);
			out.write("total number of performed merge: " + mergeCounter + "\n");
			out.flush();
			out.close();
			
		} catch (java.nio.file.NoSuchFileException e) {
			return;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}




	}

	@Deprecated
	public void compressTheCollectionTwoByTwo() {

		File f = new File(this.compressedCollectionOutputDirectoryPath);
		if(!f.exists()) {
			f.mkdirs();
		}

		//read the ranking file so to obtain the list of graphs
		Path rankPath = Paths.get(this.rankingFilePath);
		try(BufferedReader reader = Files.newBufferedReader(rankPath)) {
			//get the id of the first graph
			String line = reader.readLine();
			String[] parts = line.split(" ");
			String graphId = parts[2];

			//read the first graph
			String graphPath = this.getOriginalCollectionDirectoryPath() + "/" + graphId + ".ttl";
			Collection<Statement> fG = BlazegraphUsefulMethods.readOneGraph(graphPath);
			Model firstGraph = new TreeModel(fG);
			fG.clear();

			int counter = 0;


			//now we readline after line
			while(( (line = reader.readLine()) != null) && (counter < 1000) ){
				counter++;

				//get the next graph
				parts = line.split(" ");
				graphId = parts[2];
				graphPath = this.getOriginalCollectionDirectoryPath() + "/" + graphId + ".ttl";
				Collection<Statement> sG = BlazegraphUsefulMethods.readOneGraph(graphPath);
				Model secondGraph = new TreeModel(sG);
				sG.clear();

				//get the size of the two graphs
				if(decideIfCombine(firstGraph, secondGraph)) {
					//merge the two graphs
					firstGraph.addAll(secondGraph);

					//print the union
					BlazegraphUsefulMethods.printTheDamnGraph(firstGraph, this.compressedCollectionOutputDirectoryPath + "/" + counter + ".ttl");
				} else {
					BlazegraphUsefulMethods.printTheDamnGraph(firstGraph, this.compressedCollectionOutputDirectoryPath + "/" + counter + ".ttl");
				}

				firstGraph = secondGraph;
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}
	}

	/** This function decides if we want to combine the two graphs in a unique new graph.
	 * <p>
	 * To do so, it computes the overlapping of the two graphs. In this case, if G1
	 * and G2 are the two gaphs, the formula used to compute the percentage of overlapping is:
	 * <code>
	 * |G1 union G2| / min{|G1|, |G2|}
	 * </code>
	 * That is, the percentage of overlapping respect the smaller graph. This means that if the
	 * smaller graphs overlaps to the bigger one more than a certain precentage threshold, 
	 * we merge the two.
	 * */
	private boolean decideIfCombine(Model firstGraph, Model secondGraph) {
		//take the min between the two graphs
		int size1 = firstGraph.size();
		int size2 = secondGraph.size();
		int denominator = Math.min(size1, size2);

		//do the intersection
		Model intersection = new TreeModel();
		intersection.addAll(firstGraph);
		intersection.retainAll(secondGraph);

		int numerator = intersection.size() * 100;

		double percentage = (double) numerator / denominator;

		if(percentage > this.getThreshold())
			return true;
		else 
			return false;
	}

	/** Test main*/
	public static void main(String[] args) {
		AnswerCompressionPhase phase = new AnswerCompressionPhase();
		phase.compressTheCollectionWithThreshold();
	}

	public String getRankingFilePath() {
		return rankingFilePath;
	}

	public void setRankingFilePath(String rankingFilePath) {
		this.rankingFilePath = rankingFilePath;
	}

	public String getOriginalCollectionDirectoryPath() {
		return originalCollectionDirectoryPath;
	}

	public void setOriginalCollectionDirectoryPath(String originalCollectionDirectoryPath) {
		this.originalCollectionDirectoryPath = originalCollectionDirectoryPath;
	}

	public String getCompressedCollectionOutputDirectoryPath() {
		return compressedCollectionOutputDirectoryPath;
	}

	public void setCompressedCollectionOutputDirectoryPath(String compressedCollectionOutputDirectoryPath) {
		this.compressedCollectionOutputDirectoryPath = compressedCollectionOutputDirectoryPath;
	}

	public double getThreshold() {
		return compressionThreshold;
	}

	public void setThreshold(double threshold) {
		this.compressionThreshold = threshold;
	}

	public double getCompressionThreshold() {
		return compressionThreshold;
	}

	public void setCompressionThreshold(double compressionThreshold) {
		this.compressionThreshold = compressionThreshold;
	}

	public int getLookaheadThreshold() {
		return lookaheadThreshold;
	}

	public void setLookaheadThreshold(int lookaheadThreshold) {
		this.lookaheadThreshold = lookaheadThreshold;
	}
}
