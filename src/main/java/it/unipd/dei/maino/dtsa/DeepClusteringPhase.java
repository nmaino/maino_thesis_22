package it.unipd.dei.maino.dtsa;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import it.unipd.dei.ims.datastructure.ThreadState;
import it.unipd.dei.ims.rum.utilities.BlazegraphUsefulMethods;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.terrier.utilities.UrlUtilities;
import it.unipd.dei.ims.tsa.offline.ClusteringPhase;
import it.unipd.dei.ims.tsa.offline.ComputeTheTopKConnectivityList;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.URIImpl;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;


/**
 * This class is a tool for building subgraphs of a RDF dataset
 * in a different way w.r.t. {@link ClusteringPhase} .
 * Sources and terminals are selected as before however, the
 * process of expanding the graph has now changed.
 *
 * This algorithm focuses more on exploitation than exploration.
 *
 * In particular, during the expansion phase only one source is selected,
 * then the algorithm recursively explores the subgraph originating from the source.
 * Instead of expanding, the main idea is that now we "go deeper" into the graph
 * trying to retrieve more specific information regarding the source selected,
 * instead of getting a general knowledge around the node (By expanding everything).
 *
 * @author Nicola Maino
 * @version 1.0
 */
public class DeepClusteringPhase {

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
    private Map<String, DeepClusteringPhase.Color> nodeColorMap;

    // Path where clusters will be printed
    private String clusterDirectoryPath;

    // Total visited nodes
    private int totBlackNodes;

    // Accessory variables to print clusters out
    private int outGraphsCounter;
    private int outGraphsDirCounter;

    private static String SQL_SELECT_SOURCE_NODES =
            "SELECT node_name FROM public.node WHERE out_degree >= ?";

    private static String SQL_SELECT_TERMINAL_NODES =
            "SELECT node_name FROM public.node WHERE in_degree >= ?";

    private static String SQL_GET_OUT_NEIGHBORHOOD =
            "SELECT subject_, predicate_, object_ FROM public.triple_store WHERE (subject_ = ?);";

    private Stopwatch timer;

    public enum Color {
        white,
        black
    }

    public DeepClusteringPhase () {

        // Init
        outGraphsCounter = 0;
        outGraphsDirCounter = 0;

        totBlackNodes = 0;

        sourceNodesSet = new HashSet<String>();
        terminalNodesSet = new HashSet<String>();

        nodeColorMap = new HashMap<String, DeepClusteringPhase.Color>();

        timer = Stopwatch.createUnstarted();

        this.setup();
    }


    private void setup() {

        // Where to load file's properties to
        Map<String, String> map;

        try {

            map = PropertiesUsefulMethods.getSinglePropertyFileMap(
                    "\\Users\\Nicola Maino\\IdeaProjects\\maino_thesis_22\\" +
                            "properties\\DeepClusteringPhase.properties");

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

            SQL_SELECT_TERMINAL_NODES =
                    "SELECT node_name FROM " + this.schema + ".node WHERE in_degree >= ?";

            SQL_GET_OUT_NEIGHBORHOOD =
                    "SELECT subject_, predicate_, object_ FROM " + this.schema + ".triple_store WHERE (subject_ = ?);";


        } catch (IOException e) {

            System.err.println("Couldn't load properties.");
            e.printStackTrace();
        }
    }


    /**
     * It builds subgraphs using the DTSA algorithm.
     *
     * @param connectivityList list with the commonest predicates in the RDF datasets,
     *                         the ones to be traversed.
     * */
    private void deepTSA(List<String> connectivityList) {

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
            this.deepAggregationPhase(connection);

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
     * Core of the DTSA algorithm.
     *
     * @param connection Connection object to the RDB
     */
    private void deepAggregationPhase(Connection connection) {

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
            if (s.equals("")) { continue; }

            // Take the color
            Color sourceColor = nodeColorMap.get(s);

            if (sourceColor == Color.white) {

                //create the cluster beginning from this node source
                Model cluster = this.deepExtendCluster(connection, s, tau);

                this.printTheCluster(cluster);
            }
        }
    }

    /**
     * This method is responsible of building the cluster
     * around the given terminal node.
     *
     * @param connection Connection object to the DB
     * @param source Source node to start with
     * @param tau parameter Tau of the algorithm
     * @return The subgraph created
     */
    private Model deepExtendCluster(Connection connection, String source, int tau) {

        // Create a new empty graph
        Model cluster = new TreeModel();

        // Keeps track whether a source node to expand has been found yet
        boolean foundSource = false;

        String sourceToExpand = null;

        nodeColorMap.put(source, Color.black);
        totBlackNodes++;

        // It queries the DB to retrieve the neighborhood of the current node.
        // A list of statements (triples) is returned
        List<Statement> outcomers = this.getOutNeighboursViaDB(connection, source);

        // Outgoing edges from the considered source node
        for (Statement triple : outcomers) {

            // Get the object and predicate
            Value obj = triple.getObject();
            Value predicate = triple.getPredicate();

            Color objColor = nodeColorMap.get(obj.toString());

            // It is a literal
            if (obj instanceof Literal) {

                cluster.add(triple);
            }

            // It is an accessory node
            else if (!sourceNodesSet.contains(obj.toString())
                    && !terminalNodesSet.contains(obj.toString())) {

                cluster.add(triple);
            }

            // It is is a terminal node: add both the node and its literals + accessory nodes
            else if (terminalNodesSet.contains(obj.toString())) {

                cluster.add(triple); // Adding the node

                if (obj instanceof URI) {

                    // Retrieving the literals and accessory nodes
                    List<Statement> objList = this.getOutNeighboursViaDB(connection, obj.toString());

                    for (Statement t : objList) {

                        Value v = t.getObject();

                        if (v instanceof Literal
                                || (!sourceNodesSet.contains(v.toString())
                                    && !terminalNodesSet.contains(v.toString()) ) ) {

                            cluster.add(t); // Adding the retrieved resources
                        }
                    }
                }
            }

            // A source node hasn't been found in the neighborhood yet. Skip if found already.
            else if (!foundSource) {

                // Checking it is a valid candidate to be added
                if ((tau > 0)
                        && (objColor == Color.white)
                        && connectivityList.contains(predicate.toString())) {

                    // Checking it is source and not terminal.
                    if (sourceNodesSet.contains(obj.toString())
                            && !terminalNodesSet.contains(obj.toString())) {

                            cluster.add(triple); // Adds the source to the cluster
                            sourceToExpand = obj.toString(); // Saves the source for the expansion
                            foundSource = true; // Sets flag to source found
                    }
                }
            }
        }

        // A proper source node to be expanded has been found
        if (sourceToExpand != null) {

            // Add to the current cluster the expansion
            cluster.addAll(
                    deepExtendCluster(connection, sourceToExpand, tau - 1));
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

        DeepClusteringPhase dPhase =
                new DeepClusteringPhase();

        ComputeTheTopKConnectivityList phase2 =
                new ComputeTheTopKConnectivityList();


        List<String> connectivityList =
                phase2.getTopKConnectivityList();

        dPhase.deepTSA(connectivityList);
    }

}
