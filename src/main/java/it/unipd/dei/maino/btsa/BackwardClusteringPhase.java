package it.unipd.dei.maino.btsa;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import it.unipd.dei.ims.datastructure.ThreadState;
import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.terrier.utilities.UrlUtilities;
import it.unipd.dei.ims.tsa.offline.ComputeTheTopKConnectivityList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.*;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.URIImpl;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;


/**
 * This class's main purpose is to extend the pool of documents
 * that TSA algorithms produces. TSA builds clusters by using
 * BFS as a way of exploring the RDF dataset indeed. As a consequence,
 * all the clusters created by the system have a BFS-like shape
 * (One root and multiple leaves according to the arrows).
 * Clusters with such a shape greatly answer to some queries but don't
 * really perform well on others.
 *
 * This algorithm proceeds instead backwards, using terminal nodes as sources
 * and by walking back builds a graph with a mirrored shape w.r.t. TSA clusters
 * (Multiple "roots" and one terminal leaf + its literals).
 *
 * @author Nicola Maino
 * @version 1.0
 */
public class BackwardClusteringPhase {

    Boolean found = false;

    // To connect to the database
    private String jdbcConnectionString;
    private String username;
    private String password;

    // RDB schema name
    private String schema = "public";

    // Subgraphs' max radius
    private int tau;

    // List of the commonest predicates of the triples
    private List<String> connectivityList;

    // Determine whether nodes are sources or terminals
    private int lambdaIn, lambdaOut;

    // Set of the source and terminal modes (high out degree and high in degree)
    private Set<String> terminalNodesSet;
    private Set<String> sourceNodesSet;

    // Keeps track of the color of terminal nodes (Watch out! terminals are the new sources here)
    private Map<String, Color> nodeColorMap;

    // Path where clusters will be printed
    private String clusterDirectoryPath;

    // Total visited nodes
    private int totBlackNodes;

    // Last id of the folder TSA produced
    private int TSALastGraphId;
    private int TSALastGraphFolderId;

    // Accessory variables to print clusters out
    private int outGraphsCounter;
    private int outGraphsDirCounter;

    private static String SQL_SELECT_SOURCE_NODES =
            "SELECT node_name FROM public.node WHERE out_degree >= ?";

    private static String SQL_SELECT_TERMINAL_NODES =
            "SELECT node_name FROM public.node WHERE in_degree >= ? AND out_degree > 0";

    private static String SQL_GET_IN_NEIGHBORHOOD =
            "SELECT subject_, predicate_, object_ FROM public.triple_store WHERE (object_ = ?);";

    private static String SQL_GET_OUT_NEIGHBORHOOD =
            "SELECT subject_, predicate_, object_ FROM public.triple_store WHERE (subject_ = ?);";

    private Stopwatch timer;


    public enum Color {
        white,
        black
    }


    public BackwardClusteringPhase () {

        // Init
        outGraphsCounter = 0;
        outGraphsDirCounter = 0;

        totBlackNodes = 0;

        sourceNodesSet = new HashSet<String>();
        terminalNodesSet = new HashSet<String>();

        nodeColorMap = new HashMap<String, Color>();

        timer = Stopwatch.createUnstarted();

        this.setup();
    }

    private void setup() {

        // Where to load file's properties to
        Map<String, String> map;

        try {

            map = PropertiesUsefulMethods.getSinglePropertyFileMap(
                    "\\Users\\Nicola Maino\\IdeaProjects\\maino_thesis_22\\" +
                            "properties\\BackwardClusteringPhase.properties");

            this.jdbcConnectionString = map.get("jdbc.connection.string");
            this.username = map.get("postgres.username");
            this.password = map.get("postgres.password");

            this.tau = Integer.parseInt(map.get("tau"));
            this.lambdaIn = Integer.parseInt(map.get("lambda.in"));
            this.lambdaOut = Integer.parseInt(map.get("lambda.out"));

            this.clusterDirectoryPath = map.get("clusters.out.directory.path");

            this.schema = map.get("schema");

            this.TSALastGraphId =  Integer.parseInt(map.get("tsa.last.graph.id"));
            this.TSALastGraphFolderId = Integer.parseInt(map.get("tsa.last.graph.folder.id"));
            this.outGraphsCounter += this.TSALastGraphId;
            this.outGraphsDirCounter += this.TSALastGraphFolderId;

            // Adding the correct (just read) schema
            SQL_SELECT_SOURCE_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE out_degree >= ?";

            // No literals are selected since literals have no outgoing edges
            SQL_SELECT_TERMINAL_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE in_degree >= ? AND out_degree > 0";

            SQL_GET_IN_NEIGHBORHOOD =
                    "SELECT subject_, predicate_, object_ FROM " + this.schema + ".triple_store WHERE (object_ = ?);";

            SQL_GET_OUT_NEIGHBORHOOD =
                    "SELECT subject_, predicate_, object_ FROM " + this.schema + ".triple_store WHERE (subject_ = ?);";


        } catch (IOException e) {

            System.err.println("Couldn't load properties.");
            e.printStackTrace();
        }
    }

    /**
     * It builds subgraphs using the BTSA algorithm.
     *
     * @param connectivityList list with the commonest predicates in the RDF datasets,
     *                         the ones to be traversed.
     * */
    private void backwardTSA(List<String> connectivityList) {

        Connection connection = null;

        this.connectivityList = connectivityList;

        try {

            // Establish a connection to postgres
            connection = DriverManager.getConnection(
                    this.jdbcConnectionString,
                    this.username,
                    this.password);

            // Get sets of terminal and source nodes according to the chosen parameters
            this.computeNodesSets(connection);

            // Create the clusters
            this.backwardAggregationPhase(connection);

        } catch (SQLException e) {

            System.err.println("Couldn't connect to the DB.");
            e.printStackTrace();

        } finally {

            try {

                if (connection != null) { connection.close(); }

            } catch (SQLException e) {

                System.err.println("Couldn't close the connection to to the DB.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Core of the BTSA algorithm.
     *
     * @param connection Connection object to the RDB
     */
    private void backwardAggregationPhase(Connection connection) {

        timer.start();

        File f = new File(this.clusterDirectoryPath);

        if (!f.exists()) { f.mkdirs(); }

        // Iterator over the terminals
        for (String t : terminalNodesSet) {

            if (Thread.interrupted()) {
                ThreadState.setOffLine(false);
                return;
            }

            // Ignore empty nodes
            if (t.equals("")) { continue; }

            // Take the color
            Color terminalColor = nodeColorMap.get(t);

            if (terminalColor != Color.black) {

                nodeColorMap.put(t, Color.black);
                totBlackNodes++;

                // Build a new cluster from this terminal
                Model cluster = this.extendCluster(connection, t);

                // ... and print it on file
                this.printTheCluster(cluster);
            }
        }

    }

    /**
     * Print out the clusters.
     *
     * @param cluster cluster to be printed out
     */
    private void printTheCluster(Model cluster) {

        // Feedback printed every 1000 graphs
        if (this.outGraphsCounter % 1000 == 0) {

            System.out.println("printed " + this.outGraphsCounter + " graphs in " + timer);
        }

        // New directory every 2048 graphs
        if (this.outGraphsCounter % 2048 == 0) {

            System.out.println(this.outGraphsCounter + " clusters produced in directory "
                    + this.outGraphsDirCounter + ", visited " + totBlackNodes + " black nodes");

            this.outGraphsDirCounter++;

            (new File(this.clusterDirectoryPath + "/" + this.outGraphsDirCounter))
                    .mkdir();
        }

        this.outGraphsCounter++;

        // Write out the clusters
        String path = this.clusterDirectoryPath + "/"
                + this.outGraphsDirCounter + "/"
                + this.outGraphsCounter + ".ttl";

        BlazegraphUsefulMethods.printTheDamnGraph(cluster, path);
        if (found) {
            System.err.println("\nNODE IS @ " + this.outGraphsDirCounter + "/"
                    + this.outGraphsCounter + ".ttl\n");
            found = false;
        }
    }

    /**
     * This method is responsible of building the cluster
     * around the given terminal node.
     *
     * @param connection Connection object to the DB
     * @param terminal Terminal node to build the cluster from
     * @return The subgraph created
     */
    private Model extendCluster(Connection connection, String terminal) {

        // Create a new empty graph
        Model cluster = new TreeModel();

        // This queue keeps track of terminal nodes still to be added to the cluster.
        // Pairs of nodes and available radius left are kept.
        Queue<Pair<String, Integer>> extendingQueue =
                new LinkedList<Pair<String, Integer>>();

        // We add the first terminal to the queue ad selected tau
        extendingQueue.add(new MutablePair<String, Integer>(terminal, tau));

        // Get node by node to extend.
        // At the beginning we have the passed terminal node.
        while (!extendingQueue.isEmpty()) {

            Pair<String, Integer> tPair = extendingQueue.remove();

            // Get the terminal node from the pair
            String t = tPair.getLeft();

            // mark the node t as visited
            // nodeColorMap.put(t, Color.black);
            // totBlackNodes++;

            // Decrement the radius
            int radius = tPair.getRight() - 1;

            // It queries the DB to retrieve the neighborhood of the current node.
            // A list of statements (triples) is returned
            List<Statement> outcomers = this.getOutNeighboursViaDB(connection, t);
            List<Statement> incomers = this.getInNeighboursViaDB(connection, t);

            // Outgoing edges from the considered terminal node
            for (Statement triple : outcomers) {

                //get the object and predicate
                Value obj = triple.getObject();
                Value predicate = triple.getPredicate();

                Color objColor = nodeColorMap.get(obj.toString());

                //ACCESSORY CLOUD
                // It is a literal
                if (obj instanceof Literal) {

                    cluster.add(triple);
                }

                // It is an accessory node
                else if (!sourceNodesSet.contains(obj.toString())
                        && !terminalNodesSet.contains(obj.toString())) {

                    cluster.add(triple);
                }
            }

            // Incoming edges to the considered terminal node
            for (Statement triple : incomers) {

                // Get subject and predicate
                Value subject = triple.getSubject();
                Value predicate = triple.getPredicate();

                // TODO: in this case do we care about the color?
                Color subjColor = nodeColorMap.get(subject.toString());

                // ACCESSORY CLOUD

                // Case an accessory node has an outgoing edge to the terminal considered:
                // add it to the cluster.
                if (!sourceNodesSet.contains(subject.toString())
                        && !terminalNodesSet.contains(subject.toString())) {

                    cluster.add(triple);
                }

                // Case a source node has an outgoing edge to the terminal considered.
                // add it to the cluster together with its literals and accessory nodes.
                else if (sourceNodesSet.contains(subject.toString())) {

                    cluster.add(triple);

                    // It verifies the node actually has useful nodes to be added to the cluster.
                    if (subject instanceof URI) {

                        List<Statement> objList =
                                this.getOutNeighboursViaDB(connection, subject.toString());

                        for (Statement o : objList) {

                            Value v = o.getObject();

                            // Is it a literal or an accessory node?
                            if (v instanceof Literal
                                    || ((!sourceNodesSet.contains(v.toString())
                                        && !terminalNodesSet.contains(v.toString())))) {

                                cluster.add(o);
                            }
                        }
                    }
                }

                // END ACCESSORY CLOUD

                // Case a terminal node has and outgoing edge to the terminal considered.
                // Start the actual "walking phase" (if node is unvisited, there's still
                // available radius and the predicate has required frequency)
                else if ((radius > 0)
                       // && (subjColor != Color.black)
                        && connectivityList.contains(predicate.toString())) {

                    // Furthermore, it is a terminal node (not source)
                    if (terminalNodesSet.contains(subject.toString())
                            && !sourceNodesSet.contains(subject.toString())) { // TODO: unnecessary condition by Dosso?

                        Pair<String, Integer> uPair =
                                new MutablePair<String, Integer>(subject.toString(), radius);

                        // New node to be expanded has been found
                        extendingQueue.add(uPair);

                        cluster.add(triple);
                    }
                }
            }
        }

        return cluster;
    }

    /**
     * Retrieves the out neighborhood of the terminal node given as input.
     * see {@link it.unipd.dei.ims.tsa.offline.ClusteringPhase} for
     * an implementation for source nodes.
     *
     * @param connection Connection object to the RDB
     * @param t terminal node we want the neighborhood of
     * @return a list of statements (triples) that have t as subject
     * or null if a malfunction occurs.
     */
    private List<Statement> getOutNeighboursViaDB(Connection connection, String t) {

        ResultSet rs = null;
        List<Statement> outList = new ArrayList<Statement>();

        // It tries to query the DB
        try {

            PreparedStatement stmt =
                    connection.prepareStatement(SQL_GET_OUT_NEIGHBORHOOD);
            stmt.setString(1, t);
            rs = stmt.executeQuery();

            while (rs.next()) {

                String sbj = rs.getString(1);
                String pr = rs.getString(2);
                String obj = rs.getString(3);

                URI subject = new URIImpl(sbj);
                URI predicate = new URIImpl(pr);
                Value object;

                if (UrlUtilities.checkIfValidURL(obj)) {

                    // obj is a URL
                    object = new URIImpl(obj);

                } else {

                    // literal
                    object = BlazegraphUsefulMethods.dealWithTheObjectLiteralString(obj);
                }

                Statement statement = new StatementImpl(subject, predicate, object);
                outList.add(statement);
            }

            return outList;

        } catch (SQLException e) {

            System.err.println("Error occurred while SELECTing node's outcoming neighborhood.");
            e.printStackTrace();
        }

        return outList;
    }

    /**
     * Retrieves the in neighborhood of the terminal node given as input.
     * see {@link it.unipd.dei.ims.tsa.offline.ClusteringPhase} for
     * an implementation for source nodes.
     *
     * @param connection Connection object to the RDB
     * @param t terminal node we want the neighborhood of
     * @return a list of statements (triples) that have t as subject
     * or null if a malfunction occurs.
     */
    private List<Statement> getInNeighboursViaDB(Connection connection, String t) {

        ResultSet rs = null;
        List<Statement> inList = new ArrayList<Statement>();

        // It tries to query the DB
        try {

            PreparedStatement stmt =
                    connection.prepareStatement(SQL_GET_IN_NEIGHBORHOOD);
            stmt.setString(1, t);
            rs = stmt.executeQuery();

            while (rs.next()) {

                String sbj = rs.getString(1);
                String pr = rs.getString(2);
                String obj = rs.getString(3);


                // orlando bloom doc 488747
                // tom cruise doc 488182
                // Marlon Brando doc

                if (obj.equals("http://data.linkedmdb.org/resource/actor/29581")) {
                    System.err.println("NODE OF amy adams WAS PARSED!");
                    found = true;
                }

                URI subject = new URIImpl(sbj); // TODO: VERIFY HERE IF SUBJECT IS A URI (CAN IT BE OTHERWISE?)
                URI predicate = new URIImpl(pr);
                Value object = new URIImpl(obj); // Can't be a literal

                Statement statement = new StatementImpl(subject, predicate, object);
                inList.add(statement);
            }

            return inList;

        } catch (SQLException e) {

            System.err.println("Error occurred while SELECTing node's incoming neighborhood.");
            e.printStackTrace();
        }

        return inList;
    }

    /**
     *  The role of this method is to compute sets of source and terminal
     *  nodes via a connection to the RDB. This sets will be later used by the
     *  algorithm.
     *
     * @param connection Connection object to the RDB
     */
    private void computeNodesSets(Connection connection) {

        PreparedStatement preparedSelect;

        try {

            // Fetch the source nodes
            preparedSelect = connection.prepareStatement(SQL_SELECT_SOURCE_NODES);
            preparedSelect.setInt(1, this.lambdaOut);
            ResultSet rs = preparedSelect.executeQuery();

            int mapCounter = 0;

            while (rs.next()) {

                String nodeString = rs.getString(1);

                // Add the current source node to the map
                this.sourceNodesSet.add(nodeString);

                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " source nodes in total");

            // Fetch the terminal nodes
            preparedSelect = connection.prepareStatement(SQL_SELECT_TERMINAL_NODES);
            preparedSelect.setInt(1, this.lambdaIn);
            rs = preparedSelect.executeQuery();

            mapCounter = 0;

            while (rs.next()) {

                String nodeString = rs.getString(1);

                // Add the terminal node and sets its color to unvisited.
                this.terminalNodesSet.add(nodeString);
                this.nodeColorMap.put(nodeString, Color.white);

                mapCounter++;

                if (mapCounter % 100000 == 0) {

                    System.out.println("Selected " + mapCounter + " terminal nodes");
                }
            }

            System.out.println("Selected " + mapCounter + " terminal nodes in total");

        } catch (SQLException e) {

            System.err.println("Couldn't SELECT either source or terminal nodes. ");
            e.printStackTrace();
        }
    }

    /** Test main */
    public static void main (String[] args) {

        BackwardClusteringPhase TSA2phase =
                new BackwardClusteringPhase();

        ComputeTheTopKConnectivityList phase2 =
                new ComputeTheTopKConnectivityList();


        List<String> connectivityList =
                phase2.getTopKConnectivityList();

        TSA2phase.backwardTSA(connectivityList);
    }
}
