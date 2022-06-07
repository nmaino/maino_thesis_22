package it.unipd.dei.ims.rum.utilities;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.validator.routines.UrlValidator;

/** CODE BY DENNIS DOSSO */

/**Contains useful methods for URL and URI and IRI.
 * */
public class UrlUtilities {

    /** Returns true if the string is a valid url.
     * */
    public static boolean checkIfValidURL(String url) {
        UrlValidator urlValidator = new UrlValidator();
        return urlValidator.isValid(url);
    }

    /**Given an URI/IRI, it takes the last word(s) and returns it.
     *
     * */
    public static String takeFinalWordFromIRI(String elaborandum) {

        try {
            URL urlString = new URL(elaborandum);
            String elaboratum = urlString.getPath();

            //potrebbe anche esserci una reference alla fine dell'URL.
            //è quella che ci interessa
            String ref = urlString.getRef();
            if(ref != null)
                elaboratum = ref;

            //there could be a query at the end of the URL
            String query = urlString.getQuery();
            if(query != null) {
                return elaboratum + query;
            }

            String[] splitStrings = elaboratum.split("/");//prima prendiamo l'ultima parte del path

            if(splitStrings.length>0) {
                //questa potrebbe essere composta da più parole separate da '_', che si dividono
                String[] secondSplit = splitStrings[splitStrings.length-1].split("_");
                //si mettono i caratteri in un'unica stringa, separati da spazi
                String returnandum = "";
                for(String s : secondSplit) {
                    returnandum = returnandum + " " + s;
                }
                return returnandum.trim();
            }
            else
                return urlString.getHost();



        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.err.println("error in takeFinalWordFromIRI, something went wrong in the elaboation of this string that should be an URL: " + elaborandum);
            return "";
        }
    }

}
