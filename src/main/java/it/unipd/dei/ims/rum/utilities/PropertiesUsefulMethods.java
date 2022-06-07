/** @author Dennis Dosso */

package it.unipd.dei.ims.rum.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** CODE BY DENNIS DOSSO */

/**Contains useful methods that reads in the properties files.
 *
 * */
public class PropertiesUsefulMethods {

    /**Reads all the properties in all the propertie files
     * in the directory /properties and returns a map that contains couples
     * (key, value).
     * */
    public static Map<String, String> getProperties() {
        //list all the properties file in the directory
        File folder = new File("properties");
        InputStream input;
        Properties prop = new Properties();

        Map<String, String> map = new HashMap<String, String>();

        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; ++i) {
            if(listOfFiles[i].isFile()) {
                try {
                    //take the property file
                    input = new FileInputStream(listOfFiles[i].getPath());
                    prop.load(input);
                    //take all the keys in the file
                    Set<Object> keys = prop.keySet();
                    for(Object k : keys) {
                        String key = (String) k;
                        String value = prop.getProperty(key);

                        map.put(key, value);
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return map;
    }

    /**Returns a map with only the values of a single property
     * file passed as parameter. Can be useful to obtain a map
     * with all the path regarding a single RDF database to be read.
     *
     * @throws IOException
     *
     * @param propertyPath path of the property file containing the paths of the files to be read
     * */
    public static Map<String, String> getSinglePropertyFileMap(String propertyPath) throws IOException {

        InputStream input = null;
        Map<String, String> map = new HashMap<String, String>();
        try {
            input = new FileInputStream(propertyPath);

            // load the properties file
            Properties prop = new Properties();
            prop.load(input);

            //take all the keys in the file
            Set<Object> keys = prop.keySet();
            for(Object k : keys) {
                String key = (String) k;
                String value = prop.getProperty(key);

                map.put(key, value);
            }
        }
        catch (IOException ex) {
            ex.printStackTrace();
            throw new IOException("file not found");
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IOException("cannot close input file");
                }
            }
        }

        return map;
    }

    /** Method to read all the properties inside one property file.
     * In case no file is found, this method returns null, so be careful.
     *
     * */
    public static Map<String, String> getPropertyMap(String propertyPath) {
        try {
            return PropertiesUsefulMethods.getSinglePropertyFileMap(propertyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
