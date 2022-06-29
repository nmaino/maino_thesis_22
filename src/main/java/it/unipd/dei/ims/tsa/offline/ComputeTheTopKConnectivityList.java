package it.unipd.dei.ims.tsa.offline;

import it.unipd.dei.ims.datastructure.ConnectionHandler;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** TSA algorithm: phase 2.
 * <p>
 * This class simply utilizes the information in the database to retrieve the first
 * k more frequent label used in the graph.
 *
 *
 * */
public class ComputeTheTopKConnectivityList {

    private String jdbcConnectionString;
    private String username;
    private String password;

    /** threshold of frequency for the labels of the database.
     * We will only take the labels with frequency greater
     * or equal than this.
     * */
    private int threshold;

    /** The number that defines the top-k. We take the first k
     * predicates.
     * */
    private int k;

    /** Sql query to retrieve labels in LABEL tabel above a certain threshold.
     * */
    private static  String SQL_SELECT_CONNECTING_LABELS =
            "SELECT label_name, frequency from LABEL WHERE frequency > ?  order by frequency DESC LIMIT ?";

    private String schema = "public";

    /** Creates the object responsible for this phase.
     * Reads values from properties file properties/main.properties
     * These values are:
     * <ul>
     * <li>jdbc.connection.string the string to connect to the postresql</li>
     * <li>threshold: the threshold used to decide if a predicate is to be included or not.
     * If a predicate has a frequency above the one of the theshold, it
     * will be considered</li>
     * <li>k: the maximum number of elements that will be included in the list. THe elements are ordered </li>
     * <li>schema: the database schema where we are working</li>
     * </ul>*/
    public ComputeTheTopKConnectivityList () {
        try {
            this.setup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /** Method to setup the class
     * @throws IOException */
    private void setup() throws IOException {
        Map<String, String> map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");

        this.jdbcConnectionString = map.get("jdbc.connection.string");
        this.username = map.get("postgres.username");
        this.password = map.get("postgres.pwd");
        this.threshold = Integer.parseInt(map.get("threshold"));
        this.k = Integer.parseInt(map.get("k"));

        this.schema = map.get("schema");

        SQL_SELECT_CONNECTING_LABELS =
                "SELECT label_name, frequency from " + this.schema + ".LABEL WHERE frequency > ?  "
                        + "order by frequency DESC LIMIT ?";


    }

    public List<String> getTopKConnectivityList() {
        List<String> list = new ArrayList<String>();

        //open the connection to the database
        Connection connection = null;

        try {
            //open RDB connection
//			connection = DriverManager.getConnection(this.jdbcConnectionString);
            connection = ConnectionHandler.createConnectionAsOwner(jdbcConnectionString, username, password, this.getClass().getName());

            //perform query
            PreparedStatement preparedSelect = connection.prepareStatement(SQL_SELECT_CONNECTING_LABELS);
            preparedSelect.setInt(1, threshold);
            preparedSelect.setInt(2, k);

            ResultSet rs = preparedSelect.executeQuery();
            //get the strings and put them in the list
            while(rs.next()) {
                String value = rs.getString(1);
                list.add(value);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return list;
    }




}
