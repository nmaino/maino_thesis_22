package it.unipd.dei.ims.tsa.offline;

import it.unipd.dei.ims.datastructure.ConnectionHandler;
import it.unipd.dei.ims.datastructure.ThreadState;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.rum.utilities.UrlUtilities;
import it.unipd.dei.ims.terrier.utilities.BlazegraphUsefulMethods;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.openrdf.model.Statement;
import org.openrdf.model.*;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.URIImpl;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/** TSA algorithm: phase 3.
 * <p>
 * The creation of the clusters using the information
 * prepared in the database.
 *
 * */
public class ClusteringPhase {

    /** The url of the database where to connect. An example is:
     * jdbc:postgresql://localhost:5432/disgenet?user=postgres&password=password*/
    private String jdbcConnectionString;
    private String username;
    private String password;

    private int tau;

    private List<String> connectivityList;

    private int counter;
    private int directoryCounter;

    private boolean literalFlag;

    private int lambdaIn, lambdaOut;

    private String clusterDirectoryPath;

    /**Set of the source nodes (high out degree)*/
    private Set<String> sourceNodesSet;
    /**Set of the terminal nodes (high in degree)*/
    private Set<String> terminalNodesSet;

    /**Map to keep track of the color of the nodes*/
    private Map<String, Color> nodeColorMap;

    private static String SQL_SELECT_SOURCE_NODES = "SELECT node_name FROM node WHERE out_degree >= ?";

    private static  String SQL_SELECT_IRI_SOURCE_NODES =
            "SELECT node_name FROM node WHERE iri_out_degree >= ?";

    //	private static final String SQL_SELECT_NEW_LABELS =
    //			"select subject_ from triple_store where id_ > 1022015 order by id_";

    private static String SQL_SELECT_TERMINAL_NODES =
            "SELECT node_name FROM node WHERE in_degree >= ?";

    /** To insert a new source node into the database*/
    private static String SQL_INSERT_SOURCE_NODE;

    //total visited nodes
    private int totalBlackNodes;

    private Stopwatch timer;

    private String schema;

    public enum Color {
        white,
        gray,
        black
    }

    public ClusteringPhase () {
        //XXX
        counter = 0;//0
        //XXX
        directoryCounter = 0;//0
        totalBlackNodes = 0;

        sourceNodesSet = new HashSet<String>();
        terminalNodesSet = new HashSet<String>();
        nodeColorMap = new HashMap<String, Color>();

        timer = Stopwatch.createUnstarted();
        this.setup();
    }

    private void setup() {
        Map<String, String> map;
        try {
            map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");
            this.jdbcConnectionString = map.get("jdbc.connection.string");
            this.username = map.get("postgres.username");
            this.password = map.get("postgres.pwd");
            this.tau = Integer.parseInt(map.get("tau"));
            this.lambdaIn = Integer.parseInt(map.get("lambda.in"));
            this.lambdaOut = Integer.parseInt(map.get("lambda.out"));
            this.literalFlag = Boolean.parseBoolean(map.get("literal.flag"));
            this.clusterDirectoryPath = map.get("clusters.directory.path");


            this.schema = map.get("schema");

            //putting the schema in the strings
            SQL_SELECT_SOURCE_NODES = "SELECT node_name FROM " + this.schema +
                    ".node WHERE out_degree >= ?";

            SQL_SELECT_IRI_SOURCE_NODES = "SELECT node_name FROM " + this.schema +
                    ".node WHERE iri_out_degree >= ?";

            SQL_SELECT_TERMINAL_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE in_degree >= ?";

            SQL_INSERT_SOURCE_NODE = "INSERT INTO " + this.schema + ".source_nodes(\n" +
                    "	node_name, color)\n" +
                    "	VALUES (?, ?);";


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Executes the creation of subgraphs via the TSA (Topological - Syntactical Aggregation).
     *
     * @param connectivityList list containing the predicates that we traverse. Output
     * of the phase 2.
     *
     * */
    public void TSAAlgorithm (List<String> connectivityList) {
        Connection connection = null;
        this.connectivityList = connectivityList;

        //open RDB connection
        try {
            //XXX SQL
            //connection = ConnectionHandler.createConnectionAsOwner(jdbcConnectionString, username, password, this.getClass().getName());
            		connection = DriverManager.getConnection(this.jdbcConnectionString, username, password);
            if (connection!=null) {
                System.out.println("connection estabilished\n");
            }
            //prepare the source and terminal nodes
            this.computeNodesSets(connection, this.literalFlag);

            this.aggregationPhase(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
                //					connection.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /** Populates the sets of source nodes and terminal nodes.
     * Besides, colors all the source nodes of white.
     *
     * @param labelFlag if set to false, don't count the literal as neighborhoods when computing
     * out degree.
     * */
    private void computeNodesSets(Connection connection, boolean labelFlag) {
        System.out.println("now computing nodes sets...");
        PreparedStatement preparedSelect;
        try {
            //XXX
            //perform first query
            //			PreparedStatement preparedNewIRI = connection.prepareStatement(SQL_SELECT_NEW_LABELS);
            //			ResultSet newIRIrs = preparedNewIRI.executeQuery();
            //			List<String> newIRIList = new ArrayList<String>();
            //			while(newIRIrs.next()) {
            //				String nxt = newIRIrs.getString("subject_");
            //				newIRIList.add(nxt);
            //			}

            //perform query
            if(labelFlag) {
                preparedSelect = connection.prepareStatement(SQL_SELECT_SOURCE_NODES);
            }
            else
                preparedSelect = connection.prepareStatement(SQL_SELECT_IRI_SOURCE_NODES);

            preparedSelect.setInt(1, this.lambdaOut);
            ResultSet rs = preparedSelect.executeQuery();
            int mapCounter = 0;

            while(rs.next()) {
                String nodeString = rs.getString(1);

                //XXX
                //				if(newIRIList.contains(nodeString)) {
                this.sourceNodesSet.add(nodeString);
                this.nodeColorMap.put(nodeString, Color.white);
                mapCounter++;
                if(mapCounter%100000 == 0) {
                    System.err.println("Selected " + mapCounter + " source nodes");
                }
                //				}
            }
            System.out.println("Selected " + mapCounter + " source nodes in total");

            preparedSelect = connection.prepareStatement(SQL_SELECT_TERMINAL_NODES);
            preparedSelect.setInt(1, this.lambdaIn);
            rs = preparedSelect.executeQuery();
            mapCounter = 0;
            while(rs.next()) {
                String nodeString = rs.getString(1);
                this.terminalNodesSet.add(nodeString);
                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " terminal nodes");


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /** This method was necessary in order to scale to millions of source nodes.
     * Makes the same computations of computeNodesSets, but
     * using a RDB table, source_node, to list and keep informations about the source nodes.
     *
     * */
    private void computeNodesSetsWithTable(Connection connection, boolean labelFlag) {
        PreparedStatement preparedSelect, preparedInsert;
        //perform query to get the list of source nodes
        try {
            if(labelFlag) {
                preparedSelect = connection.prepareStatement(SQL_SELECT_SOURCE_NODES);
            }
            else
                preparedSelect = connection.prepareStatement(SQL_SELECT_IRI_SOURCE_NODES);

            preparedInsert = connection.prepareStatement(SQL_INSERT_SOURCE_NODE);

            preparedSelect.setInt(1, this.lambdaOut);
            ResultSet rs = preparedSelect.executeQuery();
            int mapCounter = 0;
            int sourceNodesCounter = 0;

            while(rs.next()) {
                //get the IRI of the node
                String nodeName = rs.getString(1);
                //insert in the database. The default color is white (0)
                preparedInsert.setString(1, nodeName);
                preparedInsert.setInt(2, 0);
                preparedInsert.addBatch();
                mapCounter++;

                if(mapCounter>=2048) {
                    //if necessary, update batch
                    preparedInsert.executeBatch();
                    preparedInsert.clearBatch();
                    sourceNodesCounter += mapCounter;
                    mapCounter = 0;
                }
            }
            if(mapCounter>=0) {
                //if necessary, update batch
                preparedInsert.executeBatch();
                preparedInsert.clearBatch();
                mapCounter = 0;
            }
            System.out.println("Selected " + sourceNodesCounter + " source nodes in total");

            //terminal nodes are usually less than the source ones.
            //maybe one day we will need a table also for these nodes
            preparedSelect = connection.prepareStatement(SQL_SELECT_TERMINAL_NODES);
            preparedSelect.setInt(1, this.lambdaIn);
            rs = preparedSelect.executeQuery();
            mapCounter = 0;
            while(rs.next()) {
                String nodeString = rs.getString(1);
                this.terminalNodesSet.add(nodeString);
                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " terminal nodes");

            //TODO: riscrivere il resto dell'algoritmo TSA prendendo in considerazione il fatto che hai
            //adesso la table di source_nodes
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void aggregationPhase (Connection connection) {
        //clean the directory where we are going to write the clusters

        timer.start();

        File f = new File(this.clusterDirectoryPath);
        if(!f.exists()) {
            f.mkdirs();
        }

        //XXX
        try {
            System.out.println("Created the directory for clusters. Now emptying it if necessary...");
            FileUtils.cleanDirectory(f);
            System.out.println("done");
        } catch (IOException e1) {
            e1.printStackTrace();
            System.err.println("unable to clean the directory");
        }

        //iterator over the source nodes
        Iterator<String> sourceIterator = sourceNodesSet.iterator();

        while(sourceIterator.hasNext()) {
            if(Thread.interrupted()) {
                ThreadState.setOffLine(false);
                return;
            }

            String source = sourceIterator.next();
            if(source.equals(""))
                //ignore the empty nodes
                continue;

            //take the color
            Color sourceColor = nodeColorMap.get(source);
            if(sourceColor != Color.black) {
                //create the cluster beginning from this node source
                Model cluster = this.extendCluster(connection, source);

                this.printTheCluster(cluster);
            }
        }

    }

    /** local method to print the cluster in the right sub-directory with the right
     * name
     * */
    private void printTheCluster(Model cluster) {

        if(this.counter%1000==0) {
            System.out.println("printed " + counter + " graphs in " + timer);
            //			timer.reset();
        }
        //create a new directory if needed
        if(this.counter%2048 == 0) {
            System.out.println(counter + " clusters produced in directory " + directoryCounter + ","
                    + " visited " + totalBlackNodes + " black nodes");
            directoryCounter++;
            (new File(this.clusterDirectoryPath + "/" + directoryCounter)).mkdir();
        }
        this.counter++;
        //write down the clusters
        String path = this.clusterDirectoryPath + "/" + directoryCounter + "/" + (counter) + ".ttl";
        BlazegraphUsefulMethods.printTheDamnGraph(cluster, path);
    }

    /** Method to extend the clusters that only uses the table triple_store in memory.
     * */
    private Model extendCluster(Connection connection, String source) {
        //create a new graph
        Model cluster = new TreeModel ();

        //queue to keep track of the nodes that has to be explore yet (the information kept
        //is the node and the radius tau that we still have)
        Queue<Pair<String, Integer>> extendingQueue = new LinkedList<Pair<String, Integer>>();
        extendingQueue.add(new MutablePair<String, Integer>(source, tau));

        //visit the cluster
        while(! extendingQueue.isEmpty()) {
            Pair<String, Integer> sPair = extendingQueue.remove();

            //take the node
            String s = sPair.getLeft();

            //mark the node s as visited (thus the clustering algorithm will end)
            nodeColorMap.put(s, Color.black);
            totalBlackNodes++;

            //keep track of how far off we are from the origin (update the radius)
            int radius = sPair.getRight() - 1;

            try {
                //query the graph about the nodes around the subject
                List<Statement> list = this.getNeighboursViaDB(connection, s);

                for(Statement triple : list) {
                    //for each element in the neighbor

                    //get the object and predicate
                    Value obj = triple.getObject();
                    Value predicate = triple.getPredicate();

                    Color objColor = nodeColorMap.get(obj.toString());

                    //ACCESSORY CLOUD
                    //if it is a simple accessory node
                    if(!sourceNodesSet.contains(obj.toString()) && !terminalNodesSet.contains(obj.toString())) {
                        //you can add the statement to the cluster, but only if it is a literal
                        cluster.add(triple);
                    } else if (terminalNodesSet.contains(obj.toString())) {
                        //else it is a terminal node, useful information anyway
                        cluster.add(triple);
                        if(obj instanceof URI) {
                            //if it is an URI, it can have other useful information to include
                            List<Statement> objList = this.getNeighboursViaDB(connection, obj.toString());

                            for(Statement t : objList ) {
                                Value v = t.getObject();
                                if(v instanceof Literal ||
                                        (!sourceNodesSet.contains(v.toString()) && !terminalNodesSet.contains(v.toString()) ) ) {
                                    cluster.add(t);
                                }
                            }
                        }
                    } //END ACCESSORY CLOUD
                    else if((radius > 0) &&
                            (objColor != Color.black) &&
                            connectivityList.contains(predicate.toString())) {
                        //'walking phase' of the clustering
                        //if the edge is explorable, the radius is still greater than 0 and the node u has not already been
                        //used as central node
                        if(sourceNodesSet.contains(obj.toString()) &&
                                !terminalNodesSet.contains(obj.toString())) {
                            //if it is a source node and not a terminal node
                            Pair<String, Integer> uPair = new MutablePair<String, Integer>(obj.toString(), radius);
                            extendingQueue.add(uPair);
                            //mark as visited
                            cluster.add(triple);
                        }
                    }//END of the walking phase
                }
                // TODO: test in the future the ability to add a black node as if it was a terminal node
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cluster;
    }

    /** Returns a List of Statements that are the triples with
     * source the node s. This method uses a copy of the RDF database
     * inside a Relational Database in order to be quick.
     * @throws SQLException
     * */
    private List<Statement> getNeighboursViaDB(Connection connection, String s) throws SQLException {
        //bad choice to put this string here. Do not do at home, boys
        String query = "SELECT subject_, predicate_, object_" +
                "	FROM " + this.schema + ".triple_store WHERE ( subject_=? );";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1, s);
        ResultSet rs = stmt.executeQuery();

        List<Statement> list = new ArrayList<Statement>();
        while(rs.next()) {
            String sbj = rs.getString(1);
            String pr = rs.getString(2);
            String obj = rs.getString(3);

            URI subject = new URIImpl(sbj);
            Value predicate = new URIImpl(pr);
            Value object;
            if(UrlUtilities.checkIfValidURL(obj)) {
                //obj is a URL
                object = new URIImpl(obj);
            }
            else {//literal
                object = BlazegraphUsefulMethods.dealWithTheObjectLiteralString(obj);
            }
            Statement stat = new StatementImpl(subject, (URI) predicate, object);
            list.add(stat);
        }
        return list;
    }


    /**test main*/
    public static void main(String[] args) {
        ComputeTheTopKConnectivityList phase2 = new ComputeTheTopKConnectivityList();
        System.out.println("list initialised");
        List<String> connectivityList = phase2.getTopKConnectivityList();
        System.out.println("list created");


        ClusteringPhase phase = new ClusteringPhase();
        System.out.println("clustering phase initialised");

        phase.TSAAlgorithm(connectivityList);
    }

    public String getClusterDirectoryPath() {
        return clusterDirectoryPath;
    }

    public void setClusterDirectoryPath(String clusterDirectoryPath) {
        this.clusterDirectoryPath = clusterDirectoryPath;
    }


}
