package it.unipd.dei.maino.utils;

import it.unipd.dei.ims.rum.utilities.PathUsefulMethods;

import java.io.*;
import java.util.List;
import java.util.Scanner;

public class WordDirectoryFinder {

    public static void main(String[] args) throws IOException {

        String keyword = "<DOCNO>463688</DOCNO>";

        List<String> listOfFiles =
                PathUsefulMethods.getListOfFiles(
                        "\\Users\\Nicola Maino\\Documents\\RDF_DATASETS\\linkedmdb\\algorithms\\TSA\\cluster_collection");

        Scanner a;

        for (String file : listOfFiles) {
            a = new Scanner(new BufferedReader(new FileReader(file)));

            while (a.hasNext()) {
                String words = a.next();
                if (words.equals(keyword)){
                    System.err.println("found an occurrence @ : " + file);
                }
            }
        }

    }
}