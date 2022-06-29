package it.unipd.dei.ims.rum.utilities;

import it.unipd.dei.ims.datastructure.DatabaseState;
import it.unipd.dei.ims.datastructure.DatabaseState.Strategy;
import it.unipd.dei.ims.terrier.utilities.StringUsefulMethods;
import org.apache.commons.validator.routines.UrlValidator;

import java.net.MalformedURLException;
import java.net.URL;

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
     * <p>
     * You should now use {@link takeWordsFromIri}, it is more generic and
     * customizable.
     * */
    public static String takeFinalWordFromIRI(String elaborandum) {

        try {
            URL urlString = new URL(elaborandum);
            String elaboratum = urlString.getPath();

            //In the case there is a reference at the end of the IRI, we take that
            String ref = urlString.getRef();
            if(ref != null)
                elaboratum = ref;

            //there could be a query at the end of the URL
            String query = urlString.getQuery();
            if(query != null) {
                return elaboratum + query;
            }

            //take the last part of the path
            String[] splitStrings = elaboratum.split("/");

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


    public static String takePathFromIri(String iri) {
        try {
            URL urlString = new URL(iri);
            String elaboratum = urlString.getPath();

            elaboratum = elaboratum.replaceAll("/", " ").replaceAll("_", " ").trim();
            elaboratum = elaboratum.replaceAll("-", " ").replaceAll("\\(", " ").replaceAll("\\)", " ");

            return elaboratum;
        } catch (MalformedURLException e) {
            System.err.println("\n*****error extrapolating words from string: " + iri
                    +"\n******\n");
            e.printStackTrace();
            return "";
        }
    }

    /**This method was created to extrapolate strings
     * from a single string of text
     * in a way that is more fit of the lubm dataset  */
    public static String takeWordsFromIRIForLUBM(String iri) {
        // the string we are going to return
        String returnandum = "";

        try {
            URL urlString = new URL(iri);

            //get the authority of this file
            String work = urlString.getAuthority();
            work = (work != null) ? work : "";

            //check if the authority is the url of the onthology
            //in that case, we won't include it in the asnwer
            if(work.equals("swat.cse.lehigh.edu")) {
                return urlString.getRef();
            }

            if(work.equals("www.w3.org") && urlString.getPath().equals("/1999/02/22-rdf-syntax-ns")) {
                return urlString.getRef();
            }

            if(work.equals("www.w3.org") && urlString.getPath().equals("/2002/07/owl")) {
                return urlString.getRef();
            }

            if(urlString.getPath().equals("/2000/01/rdf-schema") && work.equals("www.w3.org"))
                return urlString.getRef();

            //parts of the authority
            String[] parts = work.split("\\.");
            for(int i = 1; i < parts.length -1; ++i) {
                //the first part of the iri is www, the last one is edu,
                //so we keep those two out of the way
                returnandum = returnandum + " " + parts[i];
            }


            //we take the file, i.e. the part after the '/', but without the '/'
            if(urlString.getFile() != null)
                returnandum = returnandum + " " + urlString.getFile().replaceAll("/", " ");

            //In the case there is a reference at the end of the IRI, we take that
            String ref = urlString.getRef();
            if(ref != null)
                returnandum = returnandum + " " + ref;

            //there could be a query at the end of the URL
            String query = urlString.getQuery();
            if(query != null) {
                returnandum = returnandum + " " + returnandum;
            }

        } catch (MalformedURLException e) {
            System.err.print("you provided a strange iri: " + iri);
        }

        return returnandum.trim();

    }

    /**This method was created to extrapolate strings
     * from a single string of text
     * in a way that is more fit of the lubm dataset.
     * <p>
     * This is the second implementation of the method for lubm.
     * Here, we take one IRI and we return it complete. So, for example, an
     * IRI of the type:
     * <code>
     * http://www.Department0.University0.edu/GraduateStudent124
     *</code>
     *becomes:
     *<code>
     *Department0University0GraduateStudent124
     *</code>
     *this was done just to test how different implementations could influence
     *the outcome of the systems.
     **/
    public static String takeWordsFromIRIForLUBMFullIRI(String iri) {
        // the string we are going to return
        String returnandum = "";

        try {
            URL urlString = new URL(iri);

            //get the authority of this file
            String work = urlString.getAuthority();

            //check if the authority is the url of the onthology
            //in that case, we won't include it in the asnwer
            if(work.equals("swat.cse.lehigh.edu")) {
                return urlString.getRef();
            }

            if(work.equals("www.w3.org") && urlString.getPath().equals("/1999/02/22-rdf-syntax-ns")) {
                return urlString.getRef();
            }

            if(urlString.getPath().equals("/2000/01/rdf-schema") && work.equals("www.w3.org"))
                return urlString.getRef();

            //parts of the authority
            String[] parts = work.split("\\.");
            for(int i = 1; i < parts.length -1; ++i) {
                //the first part of the iri is www, the last one is edu,
                //so we keep those two out of the way
                returnandum = returnandum + parts[i];
            }


            //we take the file, i.e. the part after the '/', but without the '/'
            if(urlString.getFile() != null)
                returnandum = returnandum + urlString.getFile().replaceAll("/", "");

        } catch (MalformedURLException e) {
            System.err.print("you provided a strange iri: " + iri);
        }
        return returnandum;
    }

    public static String takeWordsFromIRIForBSBM(String iri) {
        // the string we are going to return
        String returnandum = "";

        try {
            //take the iri as url
            URL urlString = new URL(iri);

            //get the authority of this file
            String work = urlString.getAuthority();
            //distinguish different cases in this BSBM database
            if(work.equals("purl.org")) {
                work = urlString.getFile();
                //take the last element of the url
                String[] parts = work.split("/");
                //for BSBM, take the last two parts of the file
                String part1 = parts[parts.length-1];
                return part1;
            } else if(work.equals("www4.wiwiss.fu-berlin.de")) {

                work = urlString.getFile();
                String[] parts = work.split("/");
                //for BSBM, take the last two parts of the file
                if(parts.length==7) {
                    String part1 = parts[parts.length-2];
                    String part2 = parts[parts.length-1];
                    returnandum += " " + part1 + " " + part2;
                } else if(parts.length == 6) {
                    String part1 = parts[parts.length-1];
                    returnandum += " " + part1;
                }

            } else if(work.equals("www.w3.org") && urlString.getPath().equals("/2000/01/rdf-schema")) {
                returnandum += " " + urlString.getRef();
            } else if (work.equals("www.w3.org") && urlString.getPath().equals("/1999/02/22-rdf-syntax-ns")) {
                returnandum += " " + urlString.getRef();
            } else if (work.equals("www.w3.org") && urlString.getPath().equals("/2002/07/owl")) {
                returnandum += " " + urlString.getRef();
            } else if (work.equals("www.w3.org")) {
                returnandum += " " + urlString.getRef();
            } else if(work.equals("xmlns.com") && urlString.getPath().equals("/foaf/0.1/homepage")) {
                work = urlString.getFile();
                String[] parts = work.split("/");
                String part1 = parts[parts.length-1];
                returnandum += " " + part1;
            } else if(work.equals("xmlns.com") && urlString.getPath().startsWith("/foaf/0.1")) {
                work = urlString.getFile();
                String[] parts = work.split("/");
                String part1 = parts[parts.length-1];
                returnandum += " " + part1;
            } else if(work.equals("downlode.org")) {
                returnandum += " " + urlString.getRef();
            }
            else if(urlString.getPath().equals("/")) {
                returnandum += " " + urlString.getAuthority();
            } else {
                System.out.println("check this url for bsbm: " + urlString);
            }



        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        return returnandum.trim();
    }

    /** Methods of extraction of string finalized for the DBPedia database
     * */
    public static String takeWordsFromDBpediaIRI(String iri) {
        String toBeReturned = "";
        try {
            URL urlString = new URL(iri);
            String elaborandum = urlString.getPath();

            String ref = urlString.getRef();
            if(ref != null)
                return StringUsefulMethods.camelCaseBreakerToLowerCaseString(ref);

            String query = urlString.getQuery();
            if(query != null) {
                return query;
            }

            String[] parts = elaborandum.split("/");
            if(parts.length > 0) {
                //take the last part
                String last = parts[parts.length-1];
                //now, remove the underscores
                last = last.replaceAll("_", " ").replaceAll(":", " ").replaceAll(",", "");
                last = last.replace("%28", "(").replaceAll("%29", ")").replaceAll("%23", " ")
                        .replaceAll("%27", "'");
                //now divide the came case
                last = StringUsefulMethods.camelCaseBreakerToLowerCaseString(last).toLowerCase();
                return last;
            }


        } catch (MalformedURLException e) {
            e.printStackTrace();
            return "";
        }
        return toBeReturned = "";
    }

    /**This is the method you should use every time
     * you want to extract words from an IRI, provided
     * that you set the correct DatasetState
     *
     * This is a hub method that allows you to decide
     * how to extrapolate words from an iri
     * depending on the situation and the way you
     * want to create your graph.
     *
     *
     *
     * @param iri the IRI to convert in string
     * @param protocol the protocol to use to convert the string.
     *
     * */
    public static String takeWordsFromIri(String iri) {
        int protocol = DatabaseState.getState();

        if(protocol == DatabaseState.DEFAULT)
            return takeFinalWordFromIRI(iri);
        else if(protocol == DatabaseState.LUBM)
            return takeWordsFromIRIForLUBM(iri);
        else if(protocol == DatabaseState.LUBM2)
            return takeWordsFromIRIForLUBMFullIRI(iri);
        else if(protocol == DatabaseState.BSBM)
            return takeWordsFromIRIForBSBM(iri);
        else if(protocol == DatabaseState.DBPEDIA)
            return takeWordsFromDBpediaIRI(iri);

        return "";
    }

    public static String takeWordsForEntityExtraction(String iri, DatabaseState.Strategy strategy) {

        if(strategy == Strategy.DEFAULT) {
            return takeFinalWordFromIRI(iri);
        } else if(strategy == Strategy.PATH) {
            return takePathFromIri(iri);
        }


        return "";
    }

}
