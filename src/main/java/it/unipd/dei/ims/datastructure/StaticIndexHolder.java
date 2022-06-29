package it.unipd.dei.ims.datastructure;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

public class StaticIndexHolder {

	
	private static long colCardinality = 0;
	
	private static Map<String, Index> RJIndexesMap = new HashMap<String, Index>();
	
	private static int predicateCounter = 0;
	
	/** Index of the whole R_j collection*/
	private static Index RjCollectionIndex = null;
	
	private static double uniformProbability = 0;
	
	/** Average length of a Rj document*/
	private static double averageRJLength = 0;
	
	private static boolean initialized = false;
	
	public static void setupAtZero() {
		colCardinality = 0;
		RJIndexesMap = new HashMap<String, Index>();
		predicateCounter = 0;
		RjCollectionIndex = null;
		uniformProbability = 0;
		averageRJLength = 0;
		initialized = false;
	}
	
	public static void setup(long cardinality, Map<String, String> propertyMap) {
		if(!initialized) {
			StaticIndexHolder.initialComputationsWithIndexes(cardinality, propertyMap);
			initialized = true;
		}
	}
	
	public static void initialComputationsWithIndexes(long numberOfTokens, Map<String, String> propertyMap) {
		System.out.println("Setting up for the Blanco Algorithm (file R_j) please wait...");
		//cardinality of the knowledge base (Col) composed by all the subgraphs retrieved
		colCardinality = numberOfTokens;
		
		//SETUP
		//directory where all the R_j indexes are stored
		String rjIndexesDir = propertyMap.get("blanco.rj.indexes.directory");
		File rjIndxesFile = new File(rjIndexesDir);
		File[] rjIndexes = rjIndxesFile.listFiles();
		for(File file : rjIndexes) {
			//each file in this directory contains an index
			if(file.getName().equals(".DS_Store")) 
				continue;
			
			//open the index if necessary
			Index i = RJIndexesMap.get(file.getName());
			if(i == null) {
				Index idx = IndexOnDisk.createIndex(file.getAbsolutePath(), "data");
				//save into the map
				RJIndexesMap.put(file.getName(), idx);
			}
			//update the number of R_j files we have
			predicateCounter++;				
		}
		
		if(RjCollectionIndex == null) {
			String rjIndexDir = propertyMap.get("blanco.rj.index.directory");
			//open the index of the whole R_j collection and set it
			RjCollectionIndex = IndexOnDisk.createIndex(rjIndexDir, "data");
			//now the useful statistics:
			uniformProbability = (double) 1 / predicateCounter;
			averageRJLength = RjCollectionIndex.getCollectionStatistics().getAverageDocumentLength();
			System.out.println("initial computations completed");
		}
	}
	
	public static void closeAllIndexes() {
		try {
			if(RjCollectionIndex != null)
				RjCollectionIndex.close();
			
			for(Entry<String, Index> indexEntry : RJIndexesMap.entrySet()) {
				Index index = indexEntry.getValue();
				if(index != null)
					index.close();
			}
			RJIndexesMap.clear();
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
}


	public static long getColCardinality() {
		return colCardinality;
	}

	public static void setColCardinality(long colCardinality) {
		StaticIndexHolder.colCardinality = colCardinality;
	}

	public static Map<String, Index> getRJIndexesMap() {
		return RJIndexesMap;
	}

	public static void setRJIndexesMap(Map<String, Index> rJIndexesMap) {
		RJIndexesMap = rJIndexesMap;
	}

	public static int getPredicateCounter() {
		return predicateCounter;
	}

	public static void setPredicateCounter(int predicateCounter) {
		StaticIndexHolder.predicateCounter = predicateCounter;
	}

	public static Index getRjCollectionIndex() {
		return RjCollectionIndex;
	}

	public static void setRjCollectionIndex(Index rjCollectionIndex) {
		RjCollectionIndex = rjCollectionIndex;
	}

	public static double getUniformProbability() {
		return uniformProbability;
	}

	public static void setUniformProbability(double uniformProbability) {
		StaticIndexHolder.uniformProbability = uniformProbability;
	}

	public static double getAverageRJLength() {
		return averageRJLength;
	}

	public static void setAverageRJLength(double averageRJLength) {
		StaticIndexHolder.averageRJLength = averageRJLength;
	}
}
