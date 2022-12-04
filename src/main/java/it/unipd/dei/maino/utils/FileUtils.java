package it.unipd.dei.maino.utils;

import java.io.File;

public class FileUtils {

    public static void deleteFolder(File folder) {

        File[] files = folder.listFiles();

        try {
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteFolder(f);
                    } else {
                        f.delete();
                    }
                }
            }
        } catch (NullPointerException n) {
            System.err.println("File to be deleted not found.");
            n.printStackTrace();
        }
    }
}