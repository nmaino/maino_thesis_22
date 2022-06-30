package it.unipd.dei.ims.rum.utilities;

import org.terrier.indexing.Document;
import org.terrier.indexing.FileDocument;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.realtime.memory.MemoryIndex;
import org.terrier.structures.Lexicon;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**This class contains useful methods based on Terrier.
 * 
 * */
public class TerrierUsefulMethods {

	/** Given a string representing a short test, this method indexes in local
	 * memory this document and return a list of words found by terrier inside of it.
	 * THis set of words will have passed the stemming and stop-word removal
	 * phases of Terrier if specified in the properties file.
	 * 
	 * */
	public static List<String> getDocumentWordsWithTerrierAsList(String text) {
		List<String> list = new ArrayList<String>();
		
		//creation of the document from the string of text
		Document document = new FileDocument(new StringReader(text), new HashMap(), Tokeniser.getTokeniser());
		//index in local memory
		MemoryIndex memIndex = new MemoryIndex();
		//index the document
		try {
			//index it
			memIndex.indexDocument(document);
			//get the lexicon
			Lexicon<String> lex = memIndex.getLexicon();
			//retrieve the words
			for(int i = 0; i < lex.numberOfEntries(); ++i) {
				String w = lex.getIthLexiconEntry(i).getKey();
				list.add(w);
			}
			memIndex.close();
		} catch (Exception e) {
			System.err.println("[ERROR] unable to extrapolate the words from the string " + text);
			e.printStackTrace();
		}
		
		return list;
	}
	
	/** Given a string representing a short test, this method indexes in local
	 * memory this document and return a string of words obtained in this way.
	 * This set of words will have passed the stemming and stop-word removal
	 * phases of Terrier if specified in the properties file.
	 * 
	 * */
	public static String getDocumentWordsWithTerrierAsString(String text) {
		String r = "";
		//creation of the document from the string of text
		Document document = new FileDocument(new StringReader(text), new HashMap(), Tokeniser.getTokeniser());
		//index in local memory
		MemoryIndex memIndex = new MemoryIndex();
		//index the document
		try {
			//index it
			memIndex.indexDocument(document);
			//get the lexicon
			Lexicon<String> lex = memIndex.getLexicon();
			//retrieve the words
			for(int i = 0; i < lex.numberOfEntries(); ++i) {
				String w = lex.getIthLexiconEntry(i).getKey();
				r = r + " " + w;
			}
			memIndex.close();
		} catch (Exception e) {
			System.err.println("[ERROR] unable to extrapolate the words from the string " + text);
			e.printStackTrace();
		}
		
		return r;
	}
	
}
