package it.unipd.dei.ims.datastructure;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Class to handle a jdbc connection across classes.
 * This is a tentative of Singleton for my purposes.
 * */
public class ConnectionHandler {

    private static Connection connection = null;

    private static boolean connectionSet = false;

    private static String owner = null;

    /** To know if the connection is enabled.
     * */
    public static boolean isConnectionSet() {
        return connectionSet;
    }

    public static void setConnection (Connection con) {
        connection = con;
        connectionSet = true;
    }

    public static void closeConnection() throws SQLException {
        connection.close();
        connection = null;
        connectionSet = false;
    }

    public static Connection createConnection(String jdbcConnectionString) throws SQLException {
        if(connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcConnectionString);
            connectionSet = true;
        }
        return connection;
    }

    /** The class that invokes this method
     * creates the connection and sets itself as owner.
     * <p>
     * If you are not the owner it only gives back the connection
     * (that has to be already be created)
     * <p>
     * This, together with {@link closeConnectionIfOwner}, are the man methods of this class.
     * The idea is to have one method to open a connection and to keep it open and easily
     * accesssible to other classes invoked by the first main class that started
     * a process so that the connection is not open/closed multiple time.
     * The idea is that the main class should open the connection. Other classes that
     * are then invoked try to get the connection via this method, which does not create new
     * connections but only provides the one that has already been opened. The class
     * that first opened the connection also has the right to close it later.
     *
     * @param jdbcConnectionString the string to use to connect to a PostgreSQL database
     * @param o the name of the class. Every class should use its name to keep track of which class
     * is the original owner of the connection and has the right to open/close the connection.
     * */
    public static Connection createConnectionAsOwner(String jdbcConnectionString, String o) throws SQLException {
        if(connection == null || connection.isClosed()) {
            if(owner == null || o.equals(owner)) {
                connection = DriverManager.getConnection(jdbcConnectionString);
                connectionSet = true;
                owner = o;
            }
        }

        return connection;
    }

    /** Closes the connection if the name of the class
     * that invokes this method corresponds to the owner of the connection
     * */
    public static void closeConnectionIfOwner(String o) throws SQLException {
        if(o!=null && o.equals(owner)) {
            if(connection!=null)
                connection.close();
            connection = null;
        }
    }

    public static boolean checkIfConnected() throws SQLException {
        if(connection == null || connection.isClosed()) {
            connection = null;
            connectionSet = false;
        }
        return connectionSet;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static String getOwner() {
        return owner;
    }

    public static void setOwner(String owner) {
        ConnectionHandler.owner = owner;
    }




}
