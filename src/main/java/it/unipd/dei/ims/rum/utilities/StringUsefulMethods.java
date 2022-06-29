package it.unipd.dei.ims.rum.utilities;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**This class contains useful methods for strings
 * */
public class StringUsefulMethods {

	/** Provided a string with a path, returns the ID present in the name of the file.
	 * */
	public static String getIdFromFile(File f) {
		String name = f.getName();
		String regex = "\\d+";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(name);
		String ret = "";
		if(matcher.find())
			ret = matcher.group();
		return ret;
	}
	
	
	/**Given a string written in camel case (e.g. loremIpsum) returns an array of 
	 * strings that are that string broken at the upper case letters 
	 * (e.g. from loremIpsum to lorem ipsum).
	 * 
	 * */
	public static String[] camelCaseBreakerToLowerCase(String s) {
		String[] words = s.split("(?<!^)(?=[A-Z])");
		for(int i = 0; i< words.length; ++i) {
			words[i] = words[i].toLowerCase();
		}
		return words;
	}
	
	/**Given a string written in camel case (e.g. loremIpsum) returns an array of 
	 * strings that are that string broken at the upper case letters and 
	 * unified with an underscore. 
	 * (e.g. from loremIpsum to lorem_ipsum).
	 * 
	 * */
	public static String camelCaseBreakerToLowerCaseStringForURL(String s) {
		String[] words = s.split("(?<!^)(?=[A-Z])");
		String ret = words[0];
		
		for(int i = 1; i< words.length; ++i) {
			ret = ret + "_" + words[i].toLowerCase();
		}
		return ret.trim();
	}
	
	
	/**Given a string written in camel case (e.g. loremIpsum) returns an array of 
	 * strings that are that string broken at the upper case letters and 
	 * separated by space. 
	 * (e.g. from loremIpsum to 'lorem ipsum').
	 * 
	 * */
	public static String camelCaseBreakerToLowerCaseString(String s) {
		String[] words = s.split("(?<!^)(?=[A-Z])");
		String ret = words[0];
		
		for(int i = 1; i< words.length; ++i) {
			ret = ret + " " + words[i].toLowerCase();
		}
		return ret.trim();
	}
	
	
	
	public static String checkCharacterInStringForXML(String work) {
		//get rid of  possible not acceptable char in xml
		work = work.replaceAll("\\&", "&amp;");
		work = work.replaceAll("<", "&lt;");
		work = work.replaceAll(">", "&gt;");
		work = work.replaceAll("'", "&apos;");
		work = work.replaceAll("\"", "&quot;");
		work = work.replaceAll("\\.", " ").replaceAll(":", " ");
		return work;
	}
	
	public static String checkCharacterInStringForTREC(String work) {
		//get rid of  possible not acceptable char in xml
		if(work == null)
			return "";//some special case derived by particular IRI that we didn't already dealt
		//with in the UrlUtilities' methods
		
		work = work
//				.replaceAll("\\.", " ")
//				.replaceAll(":", " ")
//				.replaceAll("-", " ")
				.replaceAll("_", " ")
				.replaceAll("\\[", " ")
				.replaceAll("\\]", " ")
				.replaceAll("\\\\", " ");
		return work;
	}
	
	/** Given an RDF Literal in the form "few_words"^^(url), it retrieves the first part, without the url 
	 * at the end describing the type of data.
	 * */
	public static String getFirstPartOfRDFLiteral(String literal) {
		String regex = "\"(.+)\"(\\^\\^(.*))?";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(literal); 
		if(matcher.find()) {
			String r = matcher.group(1);
			r = r.replaceAll("\\(", " ").replaceAll("\\)", " ").replaceAll("\"\"", "").trim();
			return r;
		}
		else
			//something particular, return all to debug
			return literal.replaceAll("\\(", " ").replaceAll("\\)", " ").replaceAll("\"\"", "").trim();
	}
	
}
