package it.unipd.dei.ims.rum.rdf.disk;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.NoSuchElementException;

import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods; // By Dennis Dosso

import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb.TDBFactory;

/**
 * This class is a prototype of a substituting class for
 * @see{RDFtoRDBTripleStoreConverter.java} originally developed
 * by Dennis Dosso by utilising Blazegraph. Most of the code is his.
 * This class makes use of Jena instead (only translated from original
 * + small additions)
 *
 * This class imports a Jena Dataset stored on the disk
 * into PostgreeSQL and
 * "provides methods to convert a triples store
 * in memory into a relational database table of the kind
 * (subject, predicate, object)" - Dennis Dosso.
 *
 * Please look at @see{JenaFromRDFFileToDiskDataset} to
 * convert a RDF file to a Jena Dataset instance to be fed
 * as input for this class.
 *
 * @author Nicola Maino
 */

public class JenaRDFToRDBTripleStoreConverter {

    // Path of the database where the RDF graph is stored
    private String rdfDatabase;

    // To connect to the rdb
    private String jdbcConnectingString;
    private String username;
    private String password;

    // Schema to refer to in te rdb
    private String schema;

    // SQL command to be performed when inserting the values into the database.
    private String sqlInsert = "INSERT INTO public.triple_store(" +
            "	subject_, predicate_, object_)" +
            "	VALUES (?, ?, ?);";

    public JenaRDFToRDBTripleStoreConverter() {}

    /** Fill the field with the information contained in the
     * property file passed as parameter.
     * @throws{IOException}
     * */
    public void setupFromPropertyFile(String propertyFilePath)  {
        Map<String, String> map;
        try {
            map = PropertiesUsefulMethods.getSinglePropertyFileMap(propertyFilePath);

            this.rdfDatabase = map.get("rdf.database");
            this.jdbcConnectingString = map.get("jdbc.driver");
            this.username = map.get("postgres.username");
            this.password = map.get("postgres.pwd");
            this.schema = map.get("schema");

            sqlInsert = "INSERT INTO " + this.schema + ".triple_store(" +
                    "	subject_, predicate_, object_)" +
                    "	VALUES (?, ?, ?);";

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void jenaConversion() throws SQLException {

        // Creates a Dataset instance
        Dataset dataset =
                TDBFactory.createDataset(rdfDatabase);

        // Sets dataset on read mode
        dataset.begin(ReadWrite.READ);

        // To connect to RDB database
        Connection rdbConnection = null;

        try {
            // Opens RDB connection
            rdbConnection =
                    DriverManager.getConnection(jdbcConnectingString, username, password);

            // Prepares the insert statement
            PreparedStatement preparedInsert =
                    rdbConnection.prepareStatement(sqlInsert);

            // Gets the RDF database
            // Creates a Jena default graph model and reads data from the file
            Model model = dataset.getDefaultModel();

            // list the statements in the Model
            StmtIterator iter = model.listStatements();

            int progressiveCounter = 0;
            int totalCounter = 0;

            Stopwatch timer = Stopwatch.createStarted();

            // Iterates over all the triples
            while (iter.hasNext()) {

                // Get next statement
                Statement stmt = iter.nextStatement();

                Resource subject = stmt.getSubject();       // Get the subject
                Property predicate = stmt.getPredicate();   // Get the predicate
                RDFNode object = stmt.getObject();          // Get the object

                // Inserts them into the rdb database
                preparedInsert.setString(1, subject.toString());
                preparedInsert.setString(2, predicate.toString());
                preparedInsert.setString(3, object.toString());

                preparedInsert.addBatch();

                progressiveCounter++;

                if (progressiveCounter >= 100000) {
                    totalCounter += progressiveCounter;
                    progressiveCounter = 0;

                    preparedInsert.executeBatch();
                    System.out.println("Inserted ... " + totalCounter + " triples in " + timer);

                    preparedInsert.clearBatch();
                    timer.stop().reset().start();
                }
            } // End of while

            // Remaining triples (last shot)
            if (progressiveCounter >= 0) {
                totalCounter += progressiveCounter;

                preparedInsert.executeBatch();
                System.out.println("Last update ... " + totalCounter + " triples in " + timer);

                preparedInsert.clearBatch();
                timer.stop().reset().start();
            }

            iter.close();

        } catch (SQLException e) {
            /* DriverManager.getConnection()
            From preparedInsert.setString(),
            preparedInsert.addBatch(),
            preparedInsert.executeBatch() */
            e.printStackTrace();
        } catch (NoSuchElementException e) {
            // From iter.hasNext()
            e.printStackTrace();
        } finally {

            dataset.end();
            if (rdbConnection != null) {
                try {
                    rdbConnection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    } // jenaConversion method

    /** Test main */
    public static void main(String[] args) throws SQLException {

        JenaRDFToRDBTripleStoreConverter converter =
                new JenaRDFToRDBTripleStoreConverter();

        converter.setupFromPropertyFile(
                "/Users/Nicola Maino/IdeaProjects/maino_thesis_22/properties/JenaRDFToRDBTripleStoreConverter.properties");

        converter.jenaConversion();
    }

} // class
