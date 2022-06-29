package it.unipd.dei.ims.datastructure;

/** This class represent which database
 * we are using right now.
 * It is simply a class that contains a variable 
 * (a singleton) that we can always access to check
 * what database we are using.
 * */
public class DatabaseState {

	/** State of the database. For example, DEFAULT; or LUBM, or BSBM etc.
	 * The state is used to describe certain characteristics of the database
	 * For instance, the algorithm to extrapolate strings from the IRI
	 * is different depending on the state of the database.
	 * <p>
	 * State is not exactly the best name for this field. It is more like
	 * the name of the database. Anyway, I was too naive at the time,
	 * so it stuck.
	 * */
	private static int state = 0;
	
	private static String schema = "public"; 
	
	/**An integer to represent the size of a database*/
	private static int size = 0;
	
	/** An integer that represents the size BIG
	 * of a database*/
	public static final int BIG = 1;
	
	public static final int SMALL = 0;
	
	
	
	/**state when you are working with a RDF with no particular requirement*/
	public static final int DEFAULT = 0;
	/** state to set when you are working with lubm and
	 * we want to divide the words*/
	public static final int LUBM = 1;
	/** state to set when you are working with lubm and
	 * we want to extract the full IRIs from the nodes,
	 * i.e. long words composed by the combination
	 * of the words from the IRI. */
	public static final int LUBM2 = 2;
	
	/** A jolly state to represent thing to jump when debugging*/
	public static final int OTHER = 3;
	
	/** The BSBM state */
	public static final int BSBM = 4; 
	
	public static final int DBPEDIA = 5;
	
	/** this enum describes a strategy in which you 
	 * extrapolate words from iri. They are:
	 * <ul>
	 * <li>DEFAULT: extrapolates only the last words from an iri</li>
	 * <li>PATH: extrapolates all the words of the path, with the exception of 
	 * the base url. For example, from www.linkedmdb.ord/resource/actor/Brad_Pitt,
	 * the extrapolated virtual document is 'resource actor Brad Pitt'.</li>
	 * </ul>
	 * */
	public enum Strategy {DEFAULT,
		/**Strategy to extract words from all of the path in the url, except the domain*/
		PATH
		}
	
	
	public static int getState() {
		return state;
	}
	
	public static void setState(int s) {
		state = s;
	}
	
	/** Sets the proper state depending on the string passes as parameter
	 * The default is DEFAUL state, which trats the words inside an IRI breaking them.
	 * This method was designed to deal with the particular
	 * structure of LUBM.
	 * 
	 * */
	public static void setType(String stat) {
		if(stat==null) {
			DatabaseState.setState(DatabaseState.DEFAULT);
		}
		
		if(stat.equals("lubm")) {
			DatabaseState.setState(DatabaseState.LUBM);
		} else if(stat.equals("lubm2")) {
			//translate documents using full IRI merged in a unique word
			DatabaseState.setState(DatabaseState.LUBM2);
		} else if (stat.equals("other")) {
			DatabaseState.setState(DatabaseState.OTHER);
		} else if(stat.equals("bsbm")) {
			DatabaseState.setState(DatabaseState.BSBM);
		} else if(stat.equals("dbpedia")) {
			DatabaseState.setState(DatabaseState.DBPEDIA);
		}
		else {
			DatabaseState.setState(DatabaseState.DEFAULT);
		} 
	}
	
	/** hub method in order to be compatible with old code.
	 * It simply calls setType*/
	public static void setState(String stat) {
		setType(stat);
	}

	public static String getSchema() {
		return schema;
	}

	public static void setSchema(String schema) {
		DatabaseState.schema = schema;
	}

	public static int getSize() {
		return size;
	}

	public static void setSize(int s) {
		size = s;
	}
	
	public static void setSize(String s) {
		if(s.equals("small")) {
			size = SMALL;
		} else if (s.equals("big")) {
			size = BIG;
		} else {
			size = BIG;
		}
	}
	
	
	
}
