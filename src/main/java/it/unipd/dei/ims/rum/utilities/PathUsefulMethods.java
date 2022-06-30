package it.unipd.dei.ims.rum.utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;

public class PathUsefulMethods {

	/**Method copied from StackOverflow. Orders the elements in a map by their values in 
	 * non increasing order. The elements are paths of file containing 1 integer
	 * in their name.
	 * */
	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue(Map<K, V> map) {
		List<Entry<K, V>> list = new LinkedList<Entry<K, V>>(map.entrySet());
		Collections.sort( list, new Comparator<Entry<K, V>>() {
			public int compare(Entry<K, V> o1, Entry<K, V> o2) {
				return -(o1.getValue()).compareTo( o2.getValue() );
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
	
	/** Given the path of a directory, returns a map of copules (id, path). Path
	 * is the path of each file. The id is the ID of the file. 
	 * 
	 * <p>
	 * This method has originally been thought to be used with collections of
	 * files whose name contains an integer id. 
	 * <p>
	 * When the file path doesn't present an id a substitute is used instead.
	 * */
	public static Map<String, String> getPathsAndIDsOfAllFilesInsideDirectoryRecursively(String mainDirectory) {
		File mainDirectoryFile = new File(mainDirectory);
		
		if(! mainDirectoryFile.isDirectory()) {
			throw new IllegalArgumentException("provied path is not a directory");
		}
		
		Map<String, String> map = new HashMap<String, String>();
		
		Queue<File> fileQueue = new LinkedList<File>();
		fileQueue.add(mainDirectoryFile);
		//in case an ID is not found, we use negative id to identify the files
		int inverseCounter = -1;
		
		while(! fileQueue.isEmpty()) {
			File f = fileQueue.remove();
			File[] files = f.listFiles();
			for(File file : files) {
				if(file.isDirectory())
					fileQueue.add(file);
				else if (file.isFile() && (! file.getName().startsWith(".DS_Store"))){
					//it is a legitimate file
					//get the id
					String path = file.getAbsolutePath();
					String docId = "";
					try {
					docId = StringUsefulMethods.getIdFromFile(file);
					}
					catch(Exception e) {
						System.err.println("Error in the regular expression");
					}
					
					if(docId.equals("") || docId==null)
						docId = (inverseCounter--) + "";
					
					map.put(docId, path);
				}
			}
		}
		
		return map;
	}
	
	public static void printOrderedMap(String mainDirectory, String outputFile) {
		//get the nodes
		Map<String, String> map = PathUsefulMethods.getPathsAndIDsOfAllFilesInsideDirectoryRecursively(mainDirectory);
		//get the map
		map = MapsUsefulMethods.sortByKey(map);
		Path outputPath = Paths.get(outputFile);
		try(BufferedWriter writer = Files.newBufferedWriter(outputPath, UsefulConstants.CHARSET_ENCODING);) {
			for(Entry<String, String> entry : map.entrySet()) {
				String docId = entry.getKey();
				String path = entry.getValue();
				writer.write(docId + "," + path);
				writer.newLine();
			}
			writer.flush();
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** It does the same thing of {@link getPathsAndIDsOfAllFilesInsideDirectoryRecursively},
	 * but instead of returning a map it prints the result in a comma separated values file.
	 * 
	 * @param mainDirectory the directory where to take the files (it visits also all the subdirectories)
	 * @param outputFile The csv file where to save these values.
	 * */
	public static void getPathsAndIDsOfAllFilesInsideDirectoryRecursivelyAndPrint(String mainDirectory, String outputFile) {
		File mainDirectoryFile = new File(mainDirectory);
		
		if(! mainDirectoryFile.isDirectory()) {
			throw new IllegalArgumentException("provied path is not a directory");
		}
		
		Queue<File> fileQueue = new LinkedList<File>();
		fileQueue.add(mainDirectoryFile);
		//in case an ID is not found, we use negative id to identify the files
		int inverseCounter = -1;
		
		Path outputPath = Paths.get(outputFile);
		
		try(BufferedWriter writer = Files.newBufferedWriter(outputPath, UsefulConstants.CHARSET_ENCODING);) {
			
			while(! fileQueue.isEmpty()) {
				File f = fileQueue.remove();
				File[] files = f.listFiles();
				for(File file : files) {
					if(file.isDirectory())
						fileQueue.add(file);
					else if (file.isFile() && (! file.getName().startsWith(".DS_Store"))){
						//it is a legitimate file
						//get the id
						String path = file.getAbsolutePath();
						String docId = "";
						try {
							docId = StringUsefulMethods.getIdFromFile(file);
						}
						catch(Exception e) {
							System.err.println("Error in the regular expression");
						}
						
						if(docId.equals("") || docId==null)
							docId = (inverseCounter--) + "";
						
						writer.write(docId + "," + path);
						writer.newLine();
					}
				}
			}
			
			writer.flush();
			writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
	}
	
	/**Creates a map of paths and their respective ID reading the information from
	 * a csv file.
	 * */
	public static Map<String, String> readPathsAndIDsOfAllFilesInCSV(String csvFile) {
		File mainDirectoryFile = new File(csvFile);
		
		if(! mainDirectoryFile.isDirectory()) {
			throw new IllegalArgumentException("provied path is not a directory");
		}
		
		Map<String, String> map = new HashMap<String, String>();
		
		Queue<File> fileQueue = new LinkedList<File>();
		fileQueue.add(mainDirectoryFile);
		//in case an ID is not found, we use negative id to identify the files
		int inverseCounter = -1;
		
		while(! fileQueue.isEmpty()) {
			File f = fileQueue.remove();
			File[] files = f.listFiles();
			for(File file : files) {
				if(file.isDirectory())
					fileQueue.add(file);
				else if (file.isFile() && (! file.getName().startsWith(".DS_Store"))){
					//it is a legitimate file
					//get the id
					String path = file.getAbsolutePath();
					String docId = "";
					try {
					docId = StringUsefulMethods.getIdFromFile(file);
					}
					catch(Exception e) {
						System.err.println("Error in the regular expression");
					}
					
					if(docId.equals("") || docId==null)
						docId = (inverseCounter--) + "";
					
					map.put(docId, path);
				}
			}
		}
		
		return map;
	}
	
	
	
	/**Given the path of an rdf file, returns the language it has been written
	 * based on its name.
	 * <p>
	 * e.g. if the file is example.ttl, it returns TURTLE. If it is
	 * example.nt it returns N-TRIPLE etc.
	 * */
	public static String getTheFormatOfRDFFile(String path) {
		File file = new File(path);
		String name = file.getName();
		String[] parts = name.split("\\.");
		String format = parts[1];
		
		if(format.equals("nt"))
			return UsefulConstants.NTRIPLE;
		else if (format.equals("ttl"))
			return UsefulConstants.TURTLE;
		else
			return "";
	}
	
	/** Returns a list of path of all the files contained in the provided 
	 * directory path, including the files contained in the subdirectories
	 * (the list is obtained recursively, I know that you love this word).
	 * Except for the .DS_Store, obviously, those nasty little things.
	 * */
	public static List<String> getListOfFiles(String mainDirectory) throws IllegalArgumentException {

		List<String> list = new ArrayList<String>();
		File mainDirectoryFile = new File(mainDirectory);
		
		if(! mainDirectoryFile.isDirectory()) {
			throw new IllegalArgumentException("provied path is not a directory");
		}
		
		Queue<File> fileQueue = new LinkedList<File>();
		fileQueue.add(mainDirectoryFile);

		while(! fileQueue.isEmpty()) {
			File f = fileQueue.remove();
			File[] files = f.listFiles();
			for(File file : files) {
				if(file.isDirectory())
					//a new directory, explore
					fileQueue.add(file);
				else if (file.isFile() && (! file.getName().startsWith(".DS_Store"))){
					//it is a legitimate file
					String path = file.getAbsolutePath();
					list.add(path);
				}
			}
		}
		return list;
	}
	
	/** Returns a list with the paths of the sub-directories in a directory
	 * 
	 * @param dirPath path of the directory
	 * */
	public static File[] getListOfSubDirectoriesInADirectory(String dirPath) {
		File file = new File(dirPath);
		File[] files = file.listFiles(new FileFilter() {
		    @Override
		    public boolean accept(File f) {
		        return f.isDirectory();
		    }
		});
		return files;
	}
	
	
	
	
	
	public static void main (String[] args) {
		String mainDirectory = "/Users/Nicola Maino/Documents/RDF_DATASETS/DisGeNET/clusters/clusters";
		String outputFile = "/Users/Nicola Maino/Documents/RDF_DATASETS/DisGeNET/clusters/clusters.txt";
		PathUsefulMethods.printOrderedMap(mainDirectory, outputFile);
		System.out.print("done");
	}
	
	
	
	
	
}
