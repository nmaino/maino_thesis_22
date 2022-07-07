package it.unipd.dei.ims.tsa.offline;

import it.unipd.dei.ims.datastructure.ConnectionHandler;
import it.unipd.dei.ims.rum.utilities.MapsUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.rum.utilities.SQLUtilities;
import org.apache.jena.ext.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/** CODE BY DENNIS DOSSO */

/** TSA algorithm: phase 1.
 * <p>
 * In its first phase, the TSA algorithm explores the database in order to find
 * some important information, such as the degree of nodes.
 *
 * */
public class StatisticsComputationPhase {

    /** The number of edges inside the graph.
     * */
    private int edgeSetCardinality;

    /** Path where to find the .jnl repository (the triple store
     * created by Blazegraph).
     * */
    private String rdfDatasePath;

    /** String to use when you want to connect to the RDB database.
     * */
    private String jdbcConnectionString;
    private String username;
    private String password;

    /** If set to true, this flag counts the literals as neighbors
     * of a Resource node.
     * */
    private boolean literalFlag;

    /** Directory where to save the clusters*/
    private String clustersDirectory;

    /**This map keeps track of the frequencies of the labels in the graph.*/
    private Map<String, Integer> labelCounterMap;

    /**This map helps keeping track of the in degree of the nodes
     * */
    private Map<String, Integer> inDegreeMap;

    /**This map helps keeping track of the out degree.
     * It counts all the outgoing edges.
     * */
    private Map<String, Integer> outDegreeMapAll;

    /**This map helps keeping track of the out degree.
     * It counts only the out nodes that are URL.
     * */
    private Map<String, Integer> outDegreeMapURI;

    /**the schema of the database we are using right now*/
    private String schema = "private";

    private String SQL_GET_TRIPLE_STORE = "SELECT subject_, predicate_, object_" +
            "	FROM public.triple_store order by id_ LIMIT ? OFFSET ?;";

    /** This new string is better than the previous one.
     * I learned this way to scan big tables when I had to deal
     * with a table of 160M triples. God bless relational databases
     * */
    private static String SQL_GET_TRIPLE_STORE_OPTIMIZED = "SELECT subject_, predicate_, object_\n" +
            "FROM triple_store \n" +
            "where id_ > ?\n" + //id_ here is the offset
            "order by id_ ASC LIMIT ?;";

    /**sql string to inert a new label into the database.
     * insert into LABEL (LABEL_NAME, AVG_DEGREE, FREQUENCY) values (?, ?, ?)*/
    private static String SQL_INSERT_NEW_LABEL = "insert into LABEL (LABEL_NAME, FREQUENCY) "
            + " values (?, ?)";

    private static  String SQL_UPDATE_LABEL =
            "update LABEL set  FREQUENCY = FREQUENCY + ? WHERE LABEL_NAME=? ";

    /**String to control if a label is already present.
     * SELECT count(*) from LABEL WHERE LABEL_NAME = ?*/
    private static  String CHECK_IF_LABEL_ALREADY_PRESENT =
            "SELECT count(*) from LABEL WHERE LABEL_NAME = ?";

    private static String SQL_TRUNCATE_TABLE_LABEL = "truncate table label";
    private static String SQL_TRUNCATE_TABLE_NODE = "truncate table node";

    /** String sql to control if a node is already present.
     * <p>
     * SELECT count(*) from NODE where NODE_NAME = ?
     * */
    private static String CHECK_IF_NODE_ALREADY_PRESENT =
            "SELECT count(*) from NODE where NODE_NAME = ?::tsvector";

    /** sql string to insert a new node
     * <p>
     * insert into NODE (NODE_NAME, IN_DEGREE, OUT_DEGREE) value (?, ?, ?)
     *
     * */
    private static  String SQL_INSERT_NEW_NODE =
            "insert into NODE (NODE_NAME, IN_DEGREE, OUT_DEGREE, IRI_OUT_DEGREE) values (?, ?, ?, ?)";

    /** SQL string to update the value of a node
     * <p>
     * update NODE set IN_DEGREE = IN_DEGREE + ?, OUT_DEGREE = OUT_DEGREE + ? WHERE NODE_NAME=?
     * */
    private static  String SQL_UPDATE_NODE =
            "update NODE set IN_DEGREE = IN_DEGREE + ?, OUT_DEGREE = OUT_DEGREE + ?, IRI_OUT_DEGREE = IRI_OUT_DEGREE + ? WHERE NODE_NAME=? ";

    /** Builder of the class. Reads some parameters from
     * a property file collocated in properties/main.properties
     * The properties to prepare are:
     * <ul>
     * <li> jdbc.connection.string string to connect to PostreSQL database  </li>
     * <li> literal.flag set to true if you want to count the literals as neighbors when computing
     * the out degree </li>
     * <li> schema name of the database schema we are using. Necessary to deal with different
     * schemas and perform the queries </li>
     * </ul>
     * */
    public StatisticsComputationPhase () {

        // Initialising...
        this.labelCounterMap = new HashMap<String, Integer>();
        inDegreeMap = new HashMap<String, Integer>();
        outDegreeMapAll = new HashMap<String, Integer>();
        outDegreeMapURI = new HashMap<String, Integer>();

        try {
            this.setup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Method to setup the class
     * @throws IOException */
    private void setup() throws IOException {
        Map<String, String> map =
                PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");

        this.rdfDatasePath = map.get("rdf.dataset");
        this.jdbcConnectionString = map.get("jdbc.connection.string");
        this.username = map.get("postgres.username");
        this.password = map.get("postgres.pwd");
        this.literalFlag = Boolean.parseBoolean(map.get("literal.flag"));
        this.clustersDirectory = map.get("clusters.directory");
        this.schema = map.get("schema");

        SQL_GET_TRIPLE_STORE = "SELECT subject_, predicate_, object_" +
                "	FROM " + this.schema + ".triple_store order by id_ LIMIT ? OFFSET ?;";

        SQL_GET_TRIPLE_STORE_OPTIMIZED = "SELECT subject_, predicate_, object_\n " +
                "FROM " + this.schema + ".triple_store \n" +
                "where id_ > ?\n" + //id_ here is the offset
                "order by id_ ASC LIMIT ?;";

        SQL_TRUNCATE_TABLE_LABEL = "truncate table " + this.schema + ".label";
        SQL_TRUNCATE_TABLE_NODE = "truncate table " + this.schema +".node";

        SQL_INSERT_NEW_LABEL = "insert into " + this.schema +".LABEL (LABEL_NAME, FREQUENCY) "
                + " values (?, ?)";

        SQL_UPDATE_LABEL =
                "update " + this.schema + ".LABEL set  FREQUENCY = FREQUENCY + ? WHERE LABEL_NAME=? ";

        CHECK_IF_LABEL_ALREADY_PRESENT = "SELECT count(*) from " + this.schema + ".LABEL WHERE LABEL_NAME = ?";

        CHECK_IF_NODE_ALREADY_PRESENT = "SELECT count(*) from " + this.schema + ".NODE where NODE_NAME = ?";

        SQL_INSERT_NEW_NODE = "insert into " + this.schema + ".NODE (NODE_NAME, IN_DEGREE, OUT_DEGREE, IRI_OUT_DEGREE) values (?, ?, ?, ?)";

        SQL_UPDATE_NODE = "update " + this.schema + ".NODE set IN_DEGREE = IN_DEGREE + ?, OUT_DEGREE = OUT_DEGREE + ?, IRI_OUT_DEGREE = IRI_OUT_DEGREE + ? WHERE NODE_NAME=? ";
    }

    /** Computes the statistics of the database (in degree, general out degree, URI out degree)
     * using an RDB to iterate through the triples of the graph.
     * */
    public void computeStatisticsUsingOnlyRDB() {

        Connection connection = null;

        //open RDB connection
        try {
            connection = DriverManager.getConnection(jdbcConnectionString, username, password);
            Statement st = connection.createStatement();
            System.out.println("deleting all statistics data...");
            st.executeUpdate(SQL_TRUNCATE_TABLE_LABEL);
            System.out.println("label table truncated");
            st.executeUpdate(SQL_TRUNCATE_TABLE_NODE);
            System.out.println("node table truncated");

            //limit and offset to navigate in the triple_store table
            //XXX
            int offset = 0;//0
            int limit = 100000;

            //to keep track of our progress in order to execute a flush sometime
            Stopwatch timer = Stopwatch.createUnstarted();

            while(true) {
                //here I change offset with limit for the nature of the sql string utilized
                ResultSet iterator = SQLUtilities.executeOffsetQuery(connection, offset, limit, SQL_GET_TRIPLE_STORE_OPTIMIZED);
                //update the window
                offset = offset + limit;

                if(iterator.next()) {
                    iterator.beforeFirst();

                    //read through this block
                    while(iterator.next()) {
                        edgeSetCardinality++;

                        //take subject, predicate and object
                        String s = iterator.getString("subject_");
                        //XXX tsvector
                        //s = s.replaceAll(":", "\\\\:");
                        String p = iterator.getString("predicate_");
                        String o = iterator.getString("object_");
                        if(o.length()>2000) {
                            o = o.substring(0, 2000-1);
                        }

                        //we have seen the predicate, +1 for p
                        MapsUsefulMethods.updateSupportMap(p, this.labelCounterMap);

                        //update the out degree of the node
                        MapsUsefulMethods.updateSupportMap(s, this.outDegreeMapAll);

                        //update the URI out degree of the node
                        MapsUsefulMethods.updateSupportMapForURI(s, o, this.outDegreeMapURI);

                        MapsUsefulMethods.updateSupportMap(o, this.inDegreeMap);

                    }//end of while, we read all the block
                    //now we update the block of data in memory
                    System.out.print("Read " + this.edgeSetCardinality + " triples. Updating the database...");
                    timer.start();

                    //update the labels
                    this.batchUpdateLabelFrequencyIntoDatabaseFromMap(connection, this.labelCounterMap);
                    //update the nodes
                    this.updateNodeDegreesFromMaps(connection, outDegreeMapAll, outDegreeMapURI, inDegreeMap);

                    System.out.println("Updated in " + timer.stop());
                    timer.reset();
                } else {
                    break;
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
            e.getNextException();
        } finally {
            try {
                if (connection != null)
                    ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /** Given a map with entries (label, frequency), saves the
     * frequency into the database represented by the connection object.
     *<p>
     *Provide a map instead of single execution in order to compute less update and insert into the database.
     *At the end of the execution the map is cleaned of all its entry to free memory.
     *<p>
     *This method performs a batch insertion in order to speed up.
     *
     *@param connection connection to a jdbc database.
     *@param map Map of entries (label, int) with the frequency of the label so far.
     **/
    private void batchUpdateLabelFrequencyIntoDatabaseFromMap(Connection connection, Map<String, Integer> map) {
        try {

            //insertion statement
            PreparedStatement preparedInsert = connection.prepareStatement(SQL_INSERT_NEW_LABEL);
            //update statement
            PreparedStatement preparedUPDATE = connection.prepareStatement(SQL_UPDATE_LABEL);
            //statement to check if a label is already present
            PreparedStatement checkPrepared = connection.prepareStatement(CHECK_IF_LABEL_ALREADY_PRESENT);

            for(Entry<String, Integer> entry : map.entrySet()) {
                String label = entry.getKey();
                //check if already present
                if(label.length() > 2000) {
                    label = label.substring(0, 1999);
                    continue;
                }
                checkPrepared.setString(1, label);
                ResultSet resultSet = checkPrepared.executeQuery();
                resultSet.next();
                int result = resultSet.getInt(1);

                if(result == 0) {
                    //new entry
                    preparedInsert.setString(1, label);
                    preparedInsert.setInt(2, entry.getValue());

                    preparedInsert.addBatch();

                }
                else {
                    //entry already present
                    preparedUPDATE.setInt(1, entry.getValue());
                    if(label.length()>2000)
                        continue;
                    preparedUPDATE.setString(2, label);

                    preparedUPDATE.addBatch();
                }

            }
            preparedInsert.executeBatch();
            preparedUPDATE.executeBatch();
            map.clear();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateNodeDegreesFromMaps(Connection connection, Map<String, Integer> outDegreeMapAll,
                                           Map<String, Integer> outDegreeMapURI, Map<String, Integer> inDegreeMap) {

        try {
            //to check if a node is already present in the RDB
            PreparedStatement checkPrepared = connection.prepareStatement(CHECK_IF_NODE_ALREADY_PRESENT);

            PreparedStatement preparedInsert, preparedUpdate;
            preparedInsert = connection.prepareStatement(SQL_INSERT_NEW_NODE);
            preparedUpdate = connection.prepareStatement(SQL_UPDATE_NODE);

            //for every entry of the general out degrees
            for(Entry<String, Integer> entry : outDegreeMapAll.entrySet()) {
                //get the corresponding entry in the IRI map
                Integer uriDegree = outDegreeMapURI.get(entry.getKey());
                if(uriDegree ==  null)//in case the node is not present (only literal as neighbors? A strange possibility)
                    uriDegree = 0;
                //get the content of the node
                String label = entry.getKey();

                if(label.length()>2000) {
                    System.out.println("\ndiscarded node: " + label);
                    label = label.substring(0, 2000);
                    continue;
                }

                checkPrepared.setString(1, label);
                ResultSet resultSet = checkPrepared.executeQuery();
                resultSet.next();
                int result = resultSet.getInt(1);


                if(result == 0) {
                    //new node - insert with out degree 0 and out degree given by the map
                    preparedInsert.setString(1, label);//node name
                    preparedInsert.setInt(2, 0);//in degree
                    preparedInsert.setInt(3, entry.getValue());//out degree
//					preparedInsert.setInt(4, 0);//iri out degree
                    preparedInsert.setInt(4, uriDegree);

                    preparedInsert.addBatch();
                }
                else {
                    //node already present
                    preparedUpdate.setInt(1, 0);//in degree unchanged
                    preparedUpdate.setInt(2, entry.getValue());// out degree
                    preparedUpdate.setInt(3, uriDegree);//IRI out degree unchanged
                    preparedUpdate.setString(4, label);//node name

                    preparedUpdate.addBatch();
                }
            }
            //execute the update
            preparedInsert.executeBatch();
            preparedUpdate.executeBatch();

            preparedInsert.clearBatch();
            preparedUpdate.clearBatch();

            //for the in degree
            for(Entry<String, Integer> entry : inDegreeMap.entrySet()) {
                //check if already present
                String label = entry.getKey();

                if(label.length()>2000) {
                    System.out.println("\ndiscarded node: " + label);
                    label = label.substring(0, 2000);
                    continue;
                }

                checkPrepared.setString(1, label);
                ResultSet resultSet = checkPrepared.executeQuery();
                resultSet.next();
                int result = resultSet.getInt(1);

                if(result == 0) {
                    //new node - insert with in degree given by the map and out degree 0
                    preparedInsert.setString(1, label);
                    preparedInsert.setInt(2, entry.getValue());
                    preparedInsert.setInt(3, 0);
                    preparedInsert.setInt(4, 0);

                    preparedInsert.addBatch();
                }
                else {
                    //node already present
                    preparedUpdate.setInt(1, entry.getValue());//update in degree
                    preparedUpdate.setInt(2, 0);//keep out degree unchanged
                    preparedUpdate.setInt(3, 0);//keep the IRI out degree unchanged
                    preparedUpdate.setString(4, label);

                    preparedUpdate.addBatch();
                }
            }

            preparedInsert.executeBatch();
            preparedUpdate.executeBatch();

            preparedInsert.clearBatch();
            preparedUpdate.clearBatch();

            //clean up the memory
            outDegreeMapAll.clear();
            outDegreeMapURI.clear();
            inDegreeMap.clear();
        } catch (SQLException e) {
            System.err.println("Query written erroneously");
            e.printStackTrace();
            System.err.println(e.getNextException());
        }
    }


    /** Test main */
    public static void main(String[] args) {
        StatisticsComputationPhase execution = new StatisticsComputationPhase();
        execution.computeStatisticsUsingOnlyRDB();
    }
}
