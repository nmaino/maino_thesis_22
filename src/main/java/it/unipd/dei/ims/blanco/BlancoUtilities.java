package it.unipd.dei.ims.blanco;

import org.terrier.utility.ApplicationSetup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**This class contains useful methods to deal with the peculiarities
 * of the Blanco-Elbassuoni paper 'Keyword Search Over RDF Graphs'*/
public class BlancoUtilities {


	public static final String NT = "N-TRIPLE";/** Rappresenta la stringa da dare come parametro ai metodi Jena
	 * per leggere e scrivere i file in formato .nt (triple)*/

	public final static Charset ENCODING = StandardCharsets.UTF_8;

	public final static String SUBJECT = "subject";

	public final static String PREDICATE = "predicate";

	public final static String OBJECT = "object";

	public final static String ID = "docno";

	public static String elaborateUri(String elaborandum) {

		try {
			URL urlString = new URL(elaborandum);
			String elaboratum = urlString.getPath();

			//potrebbe anche esserci una reference alla fine dell'URL. 
			//è quella che ci interessa
			String ref = urlString.getRef();
			if(ref != null)
				elaboratum = ref;

			String[] splitStrings = elaboratum.split("/");//prima prendiamo l'ultima parte del path

			//questa potrebbe essere composta da più parole separate da '_', che si dividono
			String[] secondSplit = splitStrings[splitStrings.length-1].split("_");

			//si mettono i caratteri in un'unica stringa, separati da spazi
			String returnandum = "";
			for(String s : secondSplit) {
				returnandum = returnandum + " " + s;
			}

			return returnandum;

		} catch (MalformedURLException e) {
			e.printStackTrace();
			return "";
		}
	}
	
	/**Returns a list of String with the paths to all the files in a directory.
	 * */
	public static List<String> readAllFilesInDirectoryAndSubdirectories (String mainDirectoryPath) {
		File mainDirectory = new File(mainDirectoryPath);
		//list to be returned with the paths to all the files
		List<String> returningList = new ArrayList<String>();
		//list used to deal with the ending recursion (I'm not using recursion, for heaven's sake, but iteration) 
		Queue<String> growingList = new LinkedList<String>();
		
		if(! mainDirectory.isDirectory()) {
			throw new IllegalArgumentException("provied path is not a directory");
		}
		
		//get files in the starting directory
		File[] fileList = mainDirectory.listFiles();
		for(File file : fileList) {
			growingList.add(file.getAbsolutePath());
		}
		
		//iteration over all the files
		while( !growingList.isEmpty() ) {
			String f = growingList.poll();
			File file = new File(f);
			//if it is a file, add to the ones we return
			if(file.isFile() && (! file.getName().startsWith(".DS_Store"))) {
				//check that it is not DS_Store. I have a Mac. I know.
				returningList.add(file.getAbsolutePath());
			}
			else if (file.isDirectory()) {
				//if it is a directory, list its files and add to the queue
				File[] newFiles = file.listFiles();
				for(File nF : newFiles) {
					growingList.add(nF.getAbsolutePath());
				}
			}
		}
		
		writeFileList(returningList);
		return returningList;
	}
	
	/**Writes in a file the list passed as parameter.
	 * */
	private static void writeFileList(List<String> list) {
		String pathString = ApplicationSetup.getProperty("custom.BlancoElbassuoni.list.file.path", null);
		Path path = Paths.get(pathString);
		try(BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
			for(String graph : list) {
				writer.write(graph);
				writer.newLine();
			}
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**Extrapolates from a String that should be a path the first number in the name of the file.
	 * Used to know what number corresponds to the graph corresponding to the given path.
	 * */
	public static String getIDFromFileString(String path) {
		String[] pathParts = path.split("/");
		String fileName = pathParts[pathParts.length - 1];
		String regex = "[0-9]+";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(fileName);
		String clusterID = "";
		if(matcher.find())
			clusterID = matcher.group(0);

		return clusterID;
	}
	
	/**Divides a long list of words separated by space and returns a list of those words
	 * 
	 * */
	public static List<String> divideTheString(String stringList) {
		String[] words = stringList.split(" ");
		ArrayList<String> list = new ArrayList<String>();
		for(int i = 0; i<words.length; ++i) {
			list.add(words[i]);
		}
		return list;
	}

}
