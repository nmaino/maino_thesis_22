package it.unipd.dei.ims.rum.utilities;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapsUsefulMethods {

    /**Method copied from StackOverflow. Orders the elements in a map by their values in
     * non increasing order. The elements are paths of file containing 1 integer
     * in their name.
     * */
    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return -(o1.getValue()).compareTo( o2.getValue() );
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**Method copied from StackOverflow. Orders the elements in a map by their keys in
     * non decreasing order. The keys are supposed to be integers.
     * */
    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByKey(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                boolean what = (Integer.parseInt(o1.getKey().toString())) > ( Integer.parseInt(o2.getKey().toString()));
                if(what)
                    return 1;
                else
                    return 0;
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /** If not presents, adds a label to this map with integer value 1. If already presents,
     * updates the entry with +1.
     * <p>
     * This method is used in my projects with labels, in degrees and out degrees
     *
     * @param label the key to find into the map
     * @param map Map of (String, int) entries. If label is found, add +1 to the value, else insert a new value with
     * value 1.
     * */
    public static void updateSupportMap(String label, Map<String, Integer> map) {
        Integer value = map.get(label);

        if(value == null) {
            //new label
            map.put(label, 1);
        }
        else {
            //label already present, update with +1
            map.put(label, value + 1);
        }
    }

    /** Given a map with RDF subjects as keys and
     * integers as values, which represent the out degree
     * of the subject counting only URI and not literal,
     * this method updates the map if the
     * provided object parameter is an URI.
     * */
    public static void updateSupportMapForURI(String subject, String object, Map<String, Integer> map) {
        if(UrlUtilities.checkIfValidURL(object)) {
            Integer value = map.get(subject);

            if(value == null) {
                //new label
                map.put(subject, 1);
            }
            else {
                //label already present, update with +1
                map.put(subject, value + 1);
            }
        }
    }



}
