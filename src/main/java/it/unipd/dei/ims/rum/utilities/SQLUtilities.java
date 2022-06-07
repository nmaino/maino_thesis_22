package it.unipd.dei.ims.rum.utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

/** CODE BY DENNIS DOSSO */

public class SQLUtilities {

    /**String to control if a label is already present.
     * SELECT count(*) from LABEL WHERE LABEL_NAME = ?*/
    private static final String CHECK_IF_LABEL_ALREADY_PRESENT =
            "SELECT count(*) from LABEL WHERE LABEL_NAME = ?";

    /**sql string to inert a new label into the database.
     * insert into LABEL (LABEL_NAME, AVG_DEGREE, FREQUENCY) values (?, ?, ?)*/
    private static final String SQL_INSERT_NEW_LABEL = "insert into LABEL (LABEL_NAME, FREQUENCY) "
            + " values (?, ?)";

    private static final String SQL_UPDATE_LABEL =
            "update LABEL set  FREQUENCY = FREQUENCY + ? WHERE LABEL_NAME=? ";

    /** String sql to control if a node is already present.
     * <p>
     * SELECT count(*) from NODE where NODE_NAME = ?
     * */
    private static final String CHECK_IF_NODE_ALREADY_PRESENT =
            "SELECT count(*) from NODE where NODE_NAME = ?";

    /** sql string to insert a new node
     * <p>
     * insert into NODE (NODE_NAME, IN_DEGREE, OUT_DEGREE) value (?, ?, ?)
     *
     * */
    private static final String SQL_INSERT_NEW_NODE =
            "insert into NODE (NODE_NAME, IN_DEGREE, OUT_DEGREE, IRI_OUT_DEGREE) values (?, ?, ?, ?)";

    /** SQL string to update the value of a node
     * <p>
     * update NODE set IN_DEGREE = IN_DEGREE + ?, OUT_DEGREE = OUT_DEGREE + ? WHERE NODE_NAME=?
     * */
    private static final String SQL_UPDATE_NODE =
            "update NODE set IN_DEGREE = IN_DEGREE + ?, OUT_DEGREE = OUT_DEGREE + ?, IRI_OUT_DEGREE = IRI_OUT_DEGREE + ? WHERE NODE_NAME=? ";


    private static Connection connection;
    private static boolean allowedConnectionClosure;


    public static void setConnection(Connection con) {
        connection = con;
    }


    /** Saves into memory the information contained in the map,
     * then empties the map.
     *
     * */
    public static void updateLabelFrequencyIntoDatabaseFromMap(Connection connection, Map<String, Integer> map) {
        try {

            //insertion statement
            PreparedStatement preparedInsert = connection.prepareStatement(SQL_INSERT_NEW_LABEL);
            //update statement
            PreparedStatement preparedUPDATE = connection.prepareStatement(SQL_UPDATE_LABEL);

            PreparedStatement checkPrepared = connection.prepareStatement(CHECK_IF_LABEL_ALREADY_PRESENT);

            for(Entry<String, Integer> entry : map.entrySet()) {
                String label = entry.getKey();
                //check if already present
                checkPrepared.setString(1, label);
                ResultSet resultSet = checkPrepared.executeQuery();
                resultSet.next();
                int result = resultSet.getInt(1);

                if(result == 0) {
                    //new entry
                    preparedInsert.setString(1, label);
                    preparedInsert.setInt(2, 0);
                    preparedInsert.setInt(3, entry.getValue());

                    preparedInsert.executeUpdate();
                }
                else {
                    //entry already present
                    preparedUPDATE.setInt(1, entry.getValue());
                    preparedUPDATE.setString(2, label);

                    preparedUPDATE.executeUpdate();
                }

            }
            map.clear();
        } catch (SQLException e) {
            e.printStackTrace();
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
    public static void batchUpdateLabelFrequencyIntoDatabaseFromMap(Connection connection, Map<String, Integer> map) {
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

    /** Updates the database with the values of in degree and out degree contained in the maps and then
     * free the memory.
     *
     * <p>
     * NB: take note of the order and meaning of the parameters
     *
     * @param connection the connection to the RDB database
     * @param outDegreeMap map with entry (string, Integer) representing the out degree of the nodes
     * @param literalFlag when set to true, updates the normal in and out degree. Qhen set to false upade the in and
     * out degree that don't keep track of the literal nodes
     * */
    public static void batchUpdateDegreeInDatabaseFromMaps(Connection connection,
                                                           Map<String, Integer> outDegreeMap,
                                                           Map<String, Integer> inDegreeMap,
                                                           boolean literalFlag) {
        try {
            PreparedStatement checkPrepared = connection.prepareStatement(CHECK_IF_NODE_ALREADY_PRESENT);

            PreparedStatement preparedInsert, preparedUpdate;
            preparedInsert = connection.prepareStatement(SQL_INSERT_NEW_NODE);
            preparedUpdate = connection.prepareStatement(SQL_UPDATE_NODE);

            if(literalFlag) {
                //Cycle on the out degree
                for(Entry<String, Integer> entry : outDegreeMap.entrySet()) {
                    //check if already present
                    String label = entry.getKey();
                    checkPrepared.setString(1, label);
                    ResultSet resultSet = checkPrepared.executeQuery();
                    resultSet.next();
                    int result = resultSet.getInt(1);

                    if(result == 0) {
                        //new node - insert with out degree 0 and out degree given by the map
                        preparedInsert.setString(1, label);//node name
                        preparedInsert.setInt(2, 0);//in degree
                        preparedInsert.setInt(3, entry.getValue());//out degree
                        preparedInsert.setInt(4, 0);//iri out degree

                        preparedInsert.addBatch();
                    }
                    else {
                        //node already present
                        preparedUpdate.setInt(1, 0);//in degree unchanged
                        preparedUpdate.setInt(2, entry.getValue());// out degree
                        preparedUpdate.setInt(3, 0);//IRI out degree unchanged
                        preparedUpdate.setString(4, label);//node name

                        preparedUpdate.addBatch();
                    }
                }

                //execute the batch. I'm not sure if they can overlap in a sort of race condition
                //so I execute the batch twice in this method
                preparedInsert.executeBatch();
                preparedUpdate.executeBatch();

                preparedInsert.clearBatch();
                preparedUpdate.clearBatch();

                //cicle on the in degree
                for(Entry<String, Integer> entry : inDegreeMap.entrySet()) {
                    //check if already present
                    String label = entry.getKey();
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
                outDegreeMap.clear();
                inDegreeMap.clear();
            }
            else {
                //literaFlag set to false, update only the iri_out_degree

                //cicle on the out degree
                for(Entry<String, Integer> entry : outDegreeMap.entrySet()) {
                    //check if already present
                    String label = entry.getKey();
                    checkPrepared.setString(1, label);
                    ResultSet resultSet = checkPrepared.executeQuery();
                    resultSet.next();
                    int result = resultSet.getInt(1);

                    if(result == 0) {
                        //new node - insert with out degree 0 and out degree given by the map
                        preparedInsert.setString(1, label);//node name
                        preparedInsert.setInt(2, 0);//in degree
                        preparedInsert.setInt(3, 0);//out degree
                        preparedInsert.setInt(4, entry.getValue());//iri out degree

                        preparedInsert.addBatch();
                    }
                    else {
                        //node already present
                        preparedUpdate.setInt(1, 0);//in degree unchanged
                        preparedUpdate.setInt(2, 0);// out degree
                        preparedUpdate.setInt(3, entry.getValue());//IRI out degree unchanged
                        preparedUpdate.setString(4, label);//node name

                        preparedUpdate.addBatch();
                    }
                }

                preparedInsert.executeBatch();
                preparedUpdate.executeBatch();

                preparedInsert.clearBatch();
                preparedUpdate.clearBatch();

                //clean up the memory
                outDegreeMap.clear();
                inDegreeMap.clear();
            }

        } catch (SQLException e) {
            System.err.println("Query written erroneously");
            System.err.println(e.getNextException());
            e.printStackTrace();

        }

    }

    public static void updateNodeDegreesFromMaps(Connection connection, Map<String, Integer> outDegreeMapAll,
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

                //check if already present
                String label = entry.getKey();
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
            System.err.println(e.getNextException());
            e.printStackTrace();
        }
    }


    /** Checks if the provided label is already present in the database
     * represented by the connetion. If already present, updates it with +1.
     * If Not already present, inserts it with frequency 1.
     * <p>
     * Not very efficient.
     *
     * @param connection Connection to the RDF database where to update the information
     * @param label a String identifying the label to be updated.
     * */
    public static void updateLabelFrequency(Connection connection, String label) {
        try {
            //check if already present - count how many times the label appears in the database
            PreparedStatement checkStatement = connection.prepareStatement(CHECK_IF_LABEL_ALREADY_PRESENT);
            checkStatement.setString(1, label);
            ResultSet checkResult = checkStatement.executeQuery();
            checkResult.next();
            int result = checkResult.getInt(1);

            if(result == 0) {
                //we have a new entry to insert into the database
                PreparedStatement statement = connection.prepareStatement(SQL_INSERT_NEW_LABEL);
                statement.setString(1, label);
                statement.setInt(2, 0);
                statement.setInt(3, 1);

                statement.executeUpdate();
            }
            else {
                //label already present. We do +1
                PreparedStatement statement = connection.prepareStatement(SQL_UPDATE_LABEL);
                statement.setString(1, label);
                statement.setInt(2, 1);

                statement.executeUpdate();
            }

        } catch (SQLException e) {
            System.err.println("DEBUG: error in the evaluation of check query");
            e.printStackTrace();
        }
    }

    /** Given a connection String, returns the corresponding connection to a PostgreSQL database.
     * */
    public static Connection getRDBConnection(String connectionString) {
        Connection connection = null;

        //open connection to the database
        try {
            connection = DriverManager.getConnection(connectionString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /** Creates one ResultSet with the query passed as parameter. The ResultSet is set with the
     * options: ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE.
     * <p>
     * The method presuppose to be using an offset and a limit in the query. This
     * is intended as an explorative query to explore chunks of a table cluster by cluster.
     *
     * @param limit the limit in the number of elements to show (more in general, parameter 1)
     * @param offset the offset we are using (more in general, parameter 2)
     *
     */
    public static ResultSet executeOffsetQuery(Connection connection, int limit, int offset, String query) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(query,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stmt.setInt(1, limit);
        stmt.setInt(2, offset);

        ResultSet rs = stmt.executeQuery();

        return rs;
    }

    /** Like {@link executeOffsetQuery} but with an additional parameter to be put
     * at number 1*/
    public static ResultSet executeOffsetQueryPlus(Connection connection, int degree, int limit, int offset, String query) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(query,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stmt.setInt(1, degree);
        stmt.setInt(2, limit);
        stmt.setInt(3, offset);

        ResultSet rs = stmt.executeQuery();

        return rs;
    }
}
