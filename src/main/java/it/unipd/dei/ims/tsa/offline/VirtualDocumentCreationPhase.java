package it.unipd.dei.ims.tsa.offline;

import it.unipd.dei.ims.datastructure.ConnectionHandler;
import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.rum.utilities.SQLUtilities;
import it.unipd.dei.ims.terrier.utilities.UrlUtilities;
import it.unipd.dei.ims.tsa.datastructure.VNode;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.TreeModel;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/** NEW phase 1 offline.
 * <p>
 * This is a new phase for the new algorithm that creates a 
 * little virtual document in our graph. Here I test how much time it requires to work.
 * 
 * */
public class VirtualDocumentCreationPhase {

	/** String to connect to a PostgreSQL database
	 * */
	private String jdbcConnectionString;
	private String username;
	private String password;

	/** Directory where to print the graphs
	 * */
	private String virtualGraphsMainOutputDirectory;

	/** Where to save the graphs as trec documents*/
	private String virtualTrecCollectionDirectory;

	/** Directory where to save the index of the trec documents*/
	private String virtualTrecIndexDirectory;
	
	/** Directory where to save the bigram index */
	private String virtualBigramIndexDirectory;

	/** threshold to understand if a node is to include or not.
	 * It is the same lambda out used to create the representative subgraphs ("clusters")
	 * */
	private int lambdaOut;

	private int limit;

	private int offset;

	/** radius for the virtual documents*/
	private int tau;

	private int graphCounter,
	dirCounter;

	private Stopwatch timer;

	private static String SQL_GET_SOURCE_NODES = "select id_, node_name from node where iri_out_degree >= ? order by id_ limit ? offset ?";

	private static String SQL_GET_NEIGHBOURS = "select subject_, predicate_, object_ from triple_store where subject_ = ?";

	/** The schema we are using in an execution w.r.t. the rdb dataset
	 * */
	private String schema = "public";
	
	/** properties map*/
	private Map<String, String> pMap;

	public VirtualDocumentCreationPhase() {
		try {
			pMap = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");
			this.jdbcConnectionString = pMap.get("jdbc.connection.string");
			this.username = pMap.get("postgres.username");
			this.password = pMap.get("postgres.pwd");
			this.lambdaOut = Integer.parseInt(pMap.get("lambda.out"));
			
			this.virtualGraphsMainOutputDirectory = pMap.get("virtual.graphs.main.output.directory");

			this.virtualTrecCollectionDirectory = pMap.get("virtual.trec.collection.directory");

			this.virtualTrecIndexDirectory = pMap.get("virtual.trec.index.directory");
			
			this.virtualBigramIndexDirectory = pMap.get("virtual.bigram.trec.index.directory");

			this.timer = Stopwatch.createStarted();

			this.tau = Integer.parseInt(pMap.get("virtual.graphs.tau"));

			this.dirCounter = this.graphCounter = 0;

			this.limit = 10000;
			this.offset = 0;
			
			String terrierHome = pMap.get("terrier.home");
			String terrierEtc = pMap.get("terrier.etc");

			System.setProperty("terrier.home", terrierHome);
			System.setProperty("terrier.etc", terrierEtc);
			
			
			this.schema = pMap.get("schema");
			
			SQL_GET_SOURCE_NODES = "select id_, node_name from " + this.schema + ".node where iri_out_degree >= ? order by id_ limit ? offset ?";

			SQL_GET_NEIGHBOURS = "select subject_, predicate_, object_ from " + this.schema + ".triple_store where subject_ = ?";
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** creates the virtual documents*/
	public void createVirtualDocuments() throws IOException {

		File f = new File(this.virtualGraphsMainOutputDirectory);
		if(!f.exists()) {
			f.mkdirs();
		}

		//create a connection to the database
		Connection connection = null;
		timer = Stopwatch.createStarted();
		try {
			//connection = ConnectionHandler.createConnectionAsOwner(jdbcConnectionString, this.getClass().getName());
			connection = DriverManager.getConnection(this.jdbcConnectionString, username, password);

			while(true) {
				//get an iterator over the interesting nodes

				ResultSet iterator = SQLUtilities.executeOffsetQueryPlus(connection, lambdaOut, limit, offset, SQL_GET_SOURCE_NODES);
				offset += limit;

				if(iterator.next()) {
					//reset to the beginning
					iterator.beforeFirst();
					while(iterator.next()) {
						//take the iri of the node
						int id_ = iterator.getInt("id_"); 
						String iri = iterator.getString("node_name");
						//now create this virtual graph
						this.createOneGraph(id_, iri, connection);
					}

					System.out.println("printed " + this.graphCounter + " graphs in " + timer);
				} else {
					timer.stop();
					break;
				}

			}
			ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		//now convert them to trec documents and index them
		f = new File(this.virtualTrecCollectionDirectory);
		if(!f.exists()) {
			f.mkdirs();
		}
		FromRDFGraphsToTRECDocuments converter = new FromRDFGraphsToTRECDocuments();

		converter.setGraphsMainDirectory(this.virtualGraphsMainOutputDirectory); 
		converter.setOutputDirectory(this.virtualTrecCollectionDirectory);
		converter.convertRDFGraphsInTRECDocuments();

		//now index
		f = new File(this.virtualTrecIndexDirectory);
		if(!f.exists()) {
			f.mkdirs();
		}
		IndexerDirectoryOfTRECFiles phase72bis = new IndexerDirectoryOfTRECFiles();
		phase72bis.setDirectoryToIndex(virtualTrecCollectionDirectory);
		phase72bis.setIndexPath(this.virtualTrecIndexDirectory);
		phase72bis.index("unigram");
	}

	private void createOneGraph(int rootId, String rootIri, Connection connection) throws SQLException {
		Model graph = new TreeModel();

		//perform a BFS algorithm
		VNode source = new VNode();
		source.setIri(rootIri);
		source.setRadius(0);

		//a queue that contains IRIs to visit
		Queue<VNode> iris = new LinkedList<VNode>();
		iris.add(source);

		while(!iris.isEmpty()) {
			VNode v = iris.remove();
			String vIri = v.getIri();
			int vRadius = v.getRadius();

			//find the neighbours
			PreparedStatement ps = connection.prepareStatement(SQL_GET_NEIGHBOURS);
			ps.setString(1, vIri);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				String subj = rs.getString("subject_");
				String pred = rs.getString("predicate_");
				String obj = rs.getString("object_");

				Statement t = BlazegraphUsefulMethods.createAStatement(subj, pred, obj);
				graph.add(t);

				//add the object to the set of iris if still inside the radius
				if(vRadius + 1 > this.tau)
					continue;

				if(UrlUtilities.checkIfValidURL(obj)) {
					VNode u = new VNode();
					u.setIri(obj);
					u.setRadius(vRadius + 1);
					iris.add(u);
				}

			}
		}

		//here we have the complete graph, and we can print it
		if(graphCounter%2048==0) {
			//create a new directory
			this.dirCounter++;
			File f = new File(this.virtualGraphsMainOutputDirectory + "/" + dirCounter);
			f.mkdirs();
			System.out.println("printed " + this.graphCounter + " graphs in " + timer);
		}
		this.graphCounter++;
		String path = this.virtualGraphsMainOutputDirectory + "/" + dirCounter + "/" + rootId + ".txt";
		BlazegraphUsefulMethods.printTheDamnGraph(graph, path);

	}

	/** Converts the virtual graphs in TREC documents
	 * and indexes them both with unigrams and
	 * bigrams*/
	public void convertAndIndex() throws IOException {
		//now convert them to trec documents and index them
		File f = new File(this.virtualTrecCollectionDirectory);
		if(!f.exists()) {
			f.mkdirs();
		}
		FromRDFGraphsToTRECDocuments converter = new FromRDFGraphsToTRECDocuments();

		converter.setGraphsMainDirectory(this.virtualGraphsMainOutputDirectory); 
		converter.setOutputDirectory(this.virtualTrecCollectionDirectory);
		converter.convertRDFGraphsInTRECDocuments();

		//now index
		IndexerDirectoryOfTRECFiles phase72bis = new IndexerDirectoryOfTRECFiles();
		phase72bis.setDirectoryToIndex(virtualTrecCollectionDirectory);
		//with unigrams
		phase72bis.setIndexPath(this.virtualTrecIndexDirectory);
		phase72bis.index("unigram");
		//with bigrams
		phase72bis.setIndexPath(this.virtualBigramIndexDirectory);
		phase72bis.index("bigram");
	}



	/** test main*/
	public static void main(String[] args) throws IOException {
		VirtualDocumentCreationPhase execution = new VirtualDocumentCreationPhase();

		Stopwatch timer = Stopwatch.createStarted();
		execution.createVirtualDocuments();
		execution.convertAndIndex();
		System.out.print("total time: " + timer.stop());
	}

	public String getVirtualTrecCollectionDirectory() {
		return virtualTrecCollectionDirectory;
	}

	public void setVirtualTrecCollectionDirectory(String virtualTrecCollectionDirectory) {
		this.virtualTrecCollectionDirectory = virtualTrecCollectionDirectory;
	}

	public String getVirtualTrecIndexDirectory() {
		return virtualTrecIndexDirectory;
	}

	public void setVirtualTrecIndexDirectory(String virtualTrecIndexDirectory) {
		this.virtualTrecIndexDirectory = virtualTrecIndexDirectory;
	}
}
