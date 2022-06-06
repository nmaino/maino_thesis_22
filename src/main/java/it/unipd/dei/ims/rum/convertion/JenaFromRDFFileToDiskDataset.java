package it.unipd.dei.ims.rum.convertion;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * This class is a prototype of a substituting class for
 * @see{FromRDFFileToDiskTripleStoreImporter.java} originally developed
 * by Dennis Dosso by utilising Blazegraph.
 * This class makes use of Jena instead.
 *
 * Please make use of @see{URI_fixer.py} to fix illegal
 * URIs in the input RDF file.
 *
 * @throws{FileNotFoundException}
 * @author Nicola Maino
 */
public class JenaFromRDFFileToDiskDataset {

    public void importFromN3ToDiskDataset(String n3FilePath, String outputDatasetPath)
            throws FileNotFoundException {

        // To read the file as a stream
        FileInputStream fin;

        // Opens file and handles exceptions
        try {
            fin = new FileInputStream(n3FilePath);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Cannot open the provided file.\n");
        }

        // Creates a Dataset instance
        Dataset dataset =
                TDBFactory.createDataset(outputDatasetPath);

        // Sets dataset on write mode
        dataset.begin(ReadWrite.WRITE);

        // Creates a Jena default graph model and reads data from the file
        Model model = dataset.getDefaultModel();

        model.read(fin, null, "N3");

        // Commits and closes
        dataset.commit();
        dataset.end();
    }

    public void importFromNTToDiskDataset(String n3FilePath, String outputDatasetPath)
            throws FileNotFoundException {

        // To read the file as a stream
        FileInputStream fin;

        // Opens file and handles exceptions
        try {
            fin = new FileInputStream(n3FilePath);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Cannot open the provided file.\n");
        }

        // Creates a Dataset instance
        Dataset dataset =
                TDBFactory.createDataset(outputDatasetPath);

        // Sets dataset on write mode
        dataset.begin(ReadWrite.WRITE);

        // Creates a Jena default graph model and reads data from the file
        Model model = dataset.getDefaultModel();

        model.read(fin, null, "NT");

        // Commits and closes
        dataset.commit();
        dataset.end();
    }

    public void importFromTTLToDiskDataset(String n3FilePath, String outputDatasetPath)
            throws FileNotFoundException {

        // To read the file as a stream
        FileInputStream fin;

        // Opens file and handles exceptions
        try {
            fin = new FileInputStream(n3FilePath);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Cannot open the provided file.\n");
        }

        // Creates a Dataset instance
        Dataset dataset =
                TDBFactory.createDataset(outputDatasetPath);

        // Sets dataset on write mode
        dataset.begin(ReadWrite.WRITE);

        // Creates a Jena default graph model and reads data from the file
        Model model = dataset.getDefaultModel();

        model.read(fin, null, "TURTLE");

        // Commits and closes
        dataset.commit();
        dataset.end();
    }

    public static void main(String args[]) throws FileNotFoundException {

        String filePath = "/Users/Nicola Maino/Documents/RDF_DATASETS/linkedmdb_1m/linkedmdb_1m.nt";
        String rdfDataset = "/Users/Nicola Maino/Documents/RDF_DATASETS/linkedmdb_1m/";

        JenaFromRDFFileToDiskDataset converter =
                new JenaFromRDFFileToDiskDataset();

        converter.importFromNTToDiskDataset(filePath, rdfDataset);

        System.out.println("RDF Dataset created.\n");
    }
}
