package it.unipd.dei.maino.rtsa;

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
 * This class is a randomized version of the TSA algorithm
 * developed by Dennis Dosso.
 * White-colored source nodes to be expanded are randomly picked up
 * by an array. See {@link it.unipd.dei.ims.tsa.offline.ClusteringPhase}
 * for more pieces of information about TSA.
 *
 * @author Nicola Maino
 * @version 1.0
 */
public class RandomizedClusteringPhase {


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

    // Arraylist of source nodes (high out degree)
    private Set<String> sourceNodesSet;

    // Set of the terminal modes (high in degree)
    private Set<String> terminalNodesSet;


    // Keeps track of the color of terminal nodes
    private Map<String, Color> nodeColorMap;

    // Path where clusters will be printed
    private String clusterDirectoryPath;

    // Total visited nodes
    private int totBlackNodes;

    // Accessory variables to print clusters out
    private int outGraphsCounter;
    private int outGraphsDirCounter;

    private static String SQL_SELECT_SOURCE_NODES =
            "SELECT node_name FROM public.node WHERE out_degree >= ?";

    // Version 2 of previous query for computeNodesSetsWRDB_V2 method.
    private static String SQL_RANDOMLY_SELECT_SOURCE_NODES =
            "SELECT node_name FROM public.node WHERE out_degree >= ? ORDER BY RANDOM()";

    private static String SQL_SELECT_TERMINAL_NODES =
            "SELECT node_name FROM public.node WHERE in_degree >= ?";

    private static String SQL_GET_OUT_NEIGHBORHOOD =
            "SELECT subject_, predicate_, object_ FROM public.triple_store WHERE (subject_ = ?);";


    private static String SQL_TRUNCATE_SOURCE_NODES_TABLE =
            "TRUNCATE TABLE public.source_nodes;";

    private static String SQL_INSERT_SOURCE_NODES =
            "INSERT INTO public.source_nodes(node_name) SELECT node_name FROM public.node WHERE out_degree >= ?";

    private String SQL_COLOR_SOURCES_ALL_WHITE =
            "UPDATE public.source_nodes SET color = 0";

    private static String SQL_GET_RANDOM_WHITE_SOURCE_NODE =
            "SELECT node_name FROM public.source_nodes WHERE color = 0 ORDER BY RANDOM() LIMIT 1";

    private static String SQL_UPDATE_SOURCE_COLOR_BLACK =
            "UPDATE public.source_nodes SET color = 1 WHERE node_name = ?;";


    private Stopwatch timer;

    public enum Color {
        white,
        black
    }

    public RandomizedClusteringPhase() {

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
                            "properties\\RandomizedClusteringPhase.properties");

            this.jdbcConnectionString = map.get("jdbc.connection.string");
            this.username = map.get("postgres.username");
            this.password = map.get("postgres.password");

            this.tau = Integer.parseInt(map.get("tau"));
            this.lambdaIn = Integer.parseInt(map.get("lambda.in"));
            this.lambdaOut = Integer.parseInt(map.get("lambda.out"));

            this.clusterDirectoryPath = map.get("clusters.out.directory.path");

            this.schema = map.get("schema");

            // Adding the correct (just read) schema
            SQL_SELECT_SOURCE_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE out_degree >= ?";

            // Version 2 of previous query for computeNodesSetsWRDB_V2 method.
            SQL_RANDOMLY_SELECT_SOURCE_NODES =
                    "SELECT node_name FROM "+ this.schema + ".node WHERE out_degree >= ? ORDER BY RANDOM()";

            SQL_SELECT_TERMINAL_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE in_degree >= ?";

            SQL_GET_OUT_NEIGHBORHOOD =
                    "SELECT subject_, predicate_, object_ FROM " + this.schema + ".triple_store WHERE (subject_ = ?);";

            // following the RDB version of the algorithm... deprecated now.
            SQL_TRUNCATE_SOURCE_NODES_TABLE =
                    "TRUNCATE TABLE " + this.schema + ".source_nodes;";

            SQL_INSERT_SOURCE_NODES =
                    "INSERT INTO " + this.schema + ".source_nodes(node_name) SELECT node_name FROM "
                            + this.schema + ".node WHERE out_degree >= ?";

            SQL_COLOR_SOURCES_ALL_WHITE =
                    "UPDATE " + this.schema + ".source_nodes SET color = 0";

            SQL_GET_RANDOM_WHITE_SOURCE_NODE =
                    "SELECT node_name FROM " + this.schema + ".source_nodes WHERE color = 0 ORDER BY RANDOM() LIMIT 1";

            SQL_UPDATE_SOURCE_COLOR_BLACK =
                    "UPDATE " + this.schema + ".source_nodes SET color = 1 WHERE node_name = ?;";


        } catch (IOException e) {

            System.err.println("Couldn't load properties.");
            e.printStackTrace();
        }
    }

    /**
     * It builds subgraphs using the RTSA algorithm.
     *
     * @param connectivityList list with the commonest predicates in the RDF datasets,
     *                         the ones to be traversed.
     * */
    private void randomizedTSA(List<String> connectivityList) {

        Connection connection = null;

        this.connectivityList = connectivityList;

        try {

            // Establish a connection to postgres
            connection = DriverManager.getConnection(
                    this.jdbcConnectionString,
                    this.username,
                    this.password);

            // Get sets of terminal and source nodes according to the chosen parameters
            this.computeNodesSetsWRDB_V2(connection);

            // Create the clusters
            this.randomizedAggregationPhase(connection);

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
     * Core of the RTSA algorithm.
     *
     * @param connection Connection object to the RDB
     */
    private void randomizedAggregationPhase(Connection connection) {

        timer.start();

        File f = new File(this.clusterDirectoryPath);

        if (!f.exists()) { f.mkdirs(); }

        // Iterator over the sources
        for (String s : sourceNodesSet) {

            if (Thread.interrupted()) {
                ThreadState.setOffLine(false);
                return;
            }

            // Ignore empty nodes
            if (s.equals("")) {
                continue;
            }

            // Take the color
            Color sourceColor = nodeColorMap.get(s);

            if (sourceColor == Color.white) {

                //create the cluster beginning from this node source
                Model cluster = this.extendCluster(connection, s);

                this.printTheCluster(cluster);
            }
        }
    }

    /**
     * Core of the RTSA algorithm.
     *
     * Known bug: The first row of the resultSet is skipped because of next() call in the while loop.
     *
     * @param connection Connection object to the RDB
     */
    private void randomizedAggregationPhaseWRDB(Connection connection) {

        timer.start();

        File f = new File(this.clusterDirectoryPath);

        if (!f.exists()) { f.mkdirs(); }

        try (java.sql.Statement stmt =
                     connection.createStatement()) {

            ResultSet rs;

            // TODO: We are missing the first row in the result set here.
            // There still are available white source nodes
            while (
                    (rs = stmt.executeQuery(SQL_GET_RANDOM_WHITE_SOURCE_NODE))
                            .next()) {

                        if (Thread.interrupted()) {

                            ThreadState.setOffLine(false);
                            return;
                        }

                        // Get the source node as a string
                        String s = rs.getString(1);

                        // Skip empty nodes
                        if (s.equals("")) {
                            continue;
                        }

                        // Create the cluster beginning from this node source
                        Model cluster = this.extendCluster(connection, s);

                        this.printTheCluster(cluster);
                    }

        } catch (SQLException e) {
            e.printStackTrace();
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
    }

    /**
     * This method is responsible of building the cluster
     * around the given source node.
     *
     * @param connection Connection object to the DB
     * @param source Source node to build the cluster from
     * @return The subgraph created
     */
    private Model extendCluster(Connection connection, String source) {

        // Create a new empty graph
        Model cluster = new TreeModel();

        // This queue keeps track of terminal nodes still to be added to the cluster.
        // Pairs of nodes and available radius left are kept.
        Queue<Pair<String, Integer>> extendingQueue =
                new LinkedList<Pair<String, Integer>>();

        // We add the first source to the queue ad selected tau
        extendingQueue.add(new MutablePair<String, Integer>(source, tau));

        // Get node by node to extend.
        while (! extendingQueue.isEmpty()) {

            Pair<String, Integer> sPair = extendingQueue.remove();

            // Take the node
            String s = sPair.getLeft();

            // Mark the node s as visited (thus the clustering algorithm will end)

            PreparedStatement preparedSelect;

            try {

                preparedSelect = connection.prepareStatement(SQL_UPDATE_SOURCE_COLOR_BLACK);
                preparedSelect.setString(1, source);
                preparedSelect.executeUpdate();

                totBlackNodes++;

            } catch (SQLException e) {

                e.printStackTrace();
            }

            int radius = sPair.getRight() - 1;

            // Query the graph about the nodes around the subject
            List<Statement> list = this.getOutNeighboursViaDB(connection, s);

            for(Statement triple : list) {

                Value obj = triple.getObject();
                Value predicate = triple.getPredicate();

                Color objColor = nodeColorMap.get(obj.toString());

                // If it is a simple accessory node
                if (!sourceNodesSet.contains(obj.toString())
                        && !terminalNodesSet.contains(obj.toString())) {

                    cluster.add(triple);
                }

                // Terminal case
                else if (terminalNodesSet.contains(obj.toString())) {

                    cluster.add(triple);

                    // If it is an URI, it can have other useful information to include
                    if (obj instanceof URI) {

                        List<Statement> objList =
                                this.getOutNeighboursViaDB(connection, obj.toString());

                        for(Statement t : objList ) {

                            Value v = t.getObject();

                            if (v instanceof Literal
                                    || (!sourceNodesSet.contains(v.toString())
                                        && !terminalNodesSet.contains(v.toString()) ) ) {

                                cluster.add(t);
                            }
                        }
                    }
                }

                // Case source
                else if ((radius > 0)
                        && (objColor == Color.white)
                        && connectivityList.contains(predicate.toString())) {

                    if (sourceNodesSet.contains(obj.toString())
                            && !terminalNodesSet.contains(obj.toString())) {

                        Pair<String, Integer> uPair =
                                new MutablePair<String, Integer>(obj.toString(), radius);

                        extendingQueue.add(uPair);
                        cluster.add(triple);
                    }
                }
            }
        }

        return cluster;
    }

    /**
     * Retrieves the out neighborhood of the node given as input.
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
     *  The role of this method is to compute sets of source and terminal
     *  nodes via a connection to the RDB. This sets will be later used by the
     *  algorithm.
     *
     * @param connection Connection object to the RDB
     */
    @Deprecated
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
                this.nodeColorMap.put(nodeString, Color.white);

                mapCounter++;

                if (mapCounter % 100000 == 0) {

                    System.out.println("Selected " + mapCounter + " source nodes");
                }
            }

            System.out.println("Selected " + mapCounter + " source nodes in total");

            System.out.println("Shuffling collection for the RTSA...");

            // Collections.shuffle(sourceNodesSet);

            System.out.println("Done.");

            // Fetch the terminal nodes
            preparedSelect = connection.prepareStatement(SQL_SELECT_TERMINAL_NODES);
            preparedSelect.setInt(1, this.lambdaIn);
            rs = preparedSelect.executeQuery();

            mapCounter = 0;

            while (rs.next()) {

                String nodeString = rs.getString(1);

                // Add the terminal node and sets its color to unvisited.
                this.terminalNodesSet.add(nodeString);

                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " terminal nodes in total");

        } catch (SQLException e) {

            System.err.println("Couldn't SELECT either source or terminal nodes. ");
            e.printStackTrace();
        }
    }

    /**
     *  The previous computeNodesSets allows the poorest performance because
     *  of the usage of the ArrayList data structure (needed for shuffling).
     *
     *  This method uses and rdb instead.
     *
     * @param connection Connection object to the RDB
     */
    @Deprecated
    private void computeNodesSetsWRDB(Connection connection) {

        PreparedStatement preparedSelect;
        java.sql.Statement stmt;

        try {

            System.out.println("Building table for source nodes... \n");

            stmt = connection.createStatement();

            // Delete previous garbage
            System.out.println("Truncating source nodes table...");
            stmt.executeUpdate(SQL_TRUNCATE_SOURCE_NODES_TABLE);
            System.out.println("Truncated.");

            // Insert the source nodes into the table
            System.out.println("Inserting source nodes into table...");
            preparedSelect = connection.prepareStatement(SQL_INSERT_SOURCE_NODES);
            preparedSelect.setInt(1, this.lambdaOut);
            preparedSelect.executeUpdate();
            System.out.println("Done.");

            // Set all nodes to white at the beginning
            System.out.println("Setting all nodes to white...");
            stmt.executeUpdate(SQL_COLOR_SOURCES_ALL_WHITE);
            System.out.println("Done.");

            // Fetch the source nodes
            preparedSelect = connection.prepareStatement(SQL_SELECT_SOURCE_NODES);
            preparedSelect.setInt(1, this.lambdaOut);
            ResultSet rs = preparedSelect.executeQuery();

            int mapCounter = 0;

            System.out.println("\nBuilding data structures... \n");

            while (rs.next()) {

                String nodeString = rs.getString(1);

                // Add the current source node to the map
                this.sourceNodesSet.add(nodeString);

                mapCounter++;

                if (mapCounter % 100000 == 0) {

                    System.out.println("Selected " + mapCounter + " source nodes");
                }
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

                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " terminal nodes in total");

        } catch (SQLException e) {

            System.err.println("Couldn't set up source node table. Stacktrace follows:\n");
            e.printStackTrace();
        }
    }

    /**
     *  The role of this method is to compute sets of source and terminal
     *  nodes via a connection to the RDB. This sets will be later used by the
     *  algorithm.
     *
     * @param connection Connection object to the RDB
     */
    private void computeNodesSetsWRDB_V2(Connection connection) {

        PreparedStatement preparedSelect;

        try {

            // Fetch the source nodes
            System.out.println("Randomly fetching source nodes from db...");
            preparedSelect = connection.prepareStatement(SQL_RANDOMLY_SELECT_SOURCE_NODES);
            preparedSelect.setInt(1, this.lambdaOut);
            ResultSet rs = preparedSelect.executeQuery();
            System.out.println("Done.");

            System.out.println("\nBuilding data structures...\n");

            int mapCounter = 0;

            while (rs.next()) {

                String nodeString = rs.getString(1);

                // Add the current source node to the map
                this.sourceNodesSet.add(nodeString);
                this.nodeColorMap.put(nodeString, Color.white);

                mapCounter++;

                if (mapCounter % 100000 == 0) {

                    System.out.println("Selected " + mapCounter + " source nodes");
                }
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

                mapCounter++;
            }

            System.out.println("Selected " + mapCounter + " terminal nodes in total");

        } catch (SQLException e) {

            System.err.println("Couldn't SELECT either source or terminal nodes. ");
            e.printStackTrace();
        }
    }


    /** Test main */
    public static void main (String[] args) {

        RandomizedClusteringPhase rPhase =
                new RandomizedClusteringPhase();

        ComputeTheTopKConnectivityList phase2 =
                new ComputeTheTopKConnectivityList();


        List<String> connectivityList =
                phase2.getTopKConnectivityList();

        rPhase.randomizedTSA(connectivityList);
    }
}
