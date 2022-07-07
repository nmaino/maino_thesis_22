package it.unipd.dei.ims.tsa.main;

import it.unipd.dei.ims.datastructure.DatabaseState;
import it.unipd.dei.ims.datastructure.ThreadState;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import it.unipd.dei.ims.tsa.offline.*;
import it.unipd.dei.ims.tsa.online.AnswerCompressionPhase;
import it.unipd.dei.ims.tsa.online.AnswerRankingPhase;
import org.apache.jena.ext.com.google.common.base.Stopwatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

//import it.unid.dei.ims.tsa.online.yosi.PriorGraphScoreComputationPhase;
//import it.unid.dei.ims.tsa.online.yosi.TSAYosiAnswerRankingPhase;
/*
import it.unipd.dei.ims.tsa.online.SmartExplorationPhase;
import it.unipd.dei.ims.tsa.online.VDPWithBacktrackingPhase;
import it.unipd.dei.ims.tsa.online.VirtualDocAndPruningOnlinePhase;
import it.unipd.dei.ims.tsa.online.blanco.AnswerSubgraphsRankingPhase;
import it.unipd.dei.ims.tsa.online.blanco.RjDocumentCreationPhase;
*/
public class TSAMainClass {

    /** Timer to control the execution of the single algorithms.
     * */
    protected static Stopwatch singleMethodTimer;

    /** Timer to control the all around execution.
     * */
    protected static Stopwatch allAroundTimer;

    /** Map of properties.
     * */
    protected Map<String, String> map;


    boolean statisticsComputationFlag;
    boolean computeTopKConnectivityListFlag;
    boolean clusteringPhaseFlag;
    boolean fromClusterToTRECDocumentsFlag;
    boolean indexDirectoryOfTRECFilesFlag;
    boolean answerRankingFlag;
    boolean copyAndIndexFlag;
    boolean rjCollectionFlag;
    boolean rankAnswersFLag;
    boolean yosiRankingFlag;
    boolean offlinePhaseFlag;
    boolean onlineCommonPhaseFlag;
    boolean onlineBlancoPhaseFlag;
    boolean onlineYosiPhaseFlag;
    boolean virtualDocumentCreationFlag;
    boolean virtualDocAndPruningFlag;
    boolean onlineVirtualDocAndPruningFlag;
    boolean onlineVirtualDocAndPruningOnTheFlyFlag;
    boolean tsaVdpBackFlag;
    boolean tsaBackFlag;

    boolean mergingRequiredFlag;
    boolean indexMergedCollectionFlag;
    boolean findBestGraphsFlag;

    protected String query = "mel gibson director film";
    protected String queryId = "1";

    protected FileWriter fWriter;
    protected BufferedWriter timeWriter;

    /** The principal query directory where we operate. The subdirectories
     * represent the queries. */
    protected String mainQueryDirectory;

    /** Directory with the path of the algorithm we are dealing with*/
    protected String mainAlgorithmDirectory;

    protected String outputTimeFilePath;

    protected String jdbcConnectionString;

    protected boolean multipleQueries;

    protected String clusterDirectoryPath;

    /** the total degree of the whole collection.
     * Necessary in the Yosi part*/
    protected int totalDegree;

    /**The schema we are using right now*/
    protected String schema;

    protected String unigramIndexPath, bigramIndexPath;

    /** Builder for the class
     * */
    public TSAMainClass() {

        try {
            map = PropertiesUsefulMethods.getSinglePropertyFileMap("properties/main.properties");
            this.setup();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.setLabels();
    }

    protected void setup() throws IOException {
        this.mainAlgorithmDirectory = map.get("main.algorithm.directory");
        this.mainQueryDirectory = mainAlgorithmDirectory + "/queries";
        File f = new File(this.mainQueryDirectory);
        if(!f.exists()) {
            f.mkdirs();
        }

        this.multipleQueries = Boolean.parseBoolean(map.get("multiple.queries"));

        //create a writer which will write about the time used by the algorithm
        File d = new File(this.mainAlgorithmDirectory + "/log");
        if(!d.exists()) {
            d.mkdirs();
        }
        this.outputTimeFilePath = this.mainAlgorithmDirectory + "/log/" + map.get("schema") + "_times.txt";
        this.fWriter = new FileWriter(this.outputTimeFilePath, true);
        this.timeWriter = new BufferedWriter(fWriter);
        timeWriter.write("\n---------- NEW EXECUTION ----------\n");

        singleMethodTimer = Stopwatch.createUnstarted();
        allAroundTimer = Stopwatch.createUnstarted();

        this.jdbcConnectionString = map.get("jdbc.connection.string");

        this.clusterDirectoryPath = map.get("cluster.directory.path");

        this.schema = map.get("schema");

        this.unigramIndexPath = map.get("unigram.index.path");
        this.bigramIndexPath = map.get("bigram.index.path");

        System.setProperty("terrier.home", map.get("terrier.home"));
        System.setProperty("terrier.etc", map.get("terrier.etc"));
        System.setProperty("terrier.var", map.get("terrier.var"));

    }

    protected void setLabels () {

        statisticsComputationFlag = Boolean.parseBoolean( map.get("statistics.computation.flag") );
        computeTopKConnectivityListFlag = Boolean.parseBoolean( map.get("compute.top.k.connectivity.list.flag") );
        clusteringPhaseFlag = Boolean.parseBoolean( map.get("clustering.phase.flag") );
        fromClusterToTRECDocumentsFlag = Boolean.parseBoolean( map.get("from.cluster.to.trec.documents.flag") );
        indexDirectoryOfTRECFilesFlag = Boolean.parseBoolean( map.get("index.directory.of.trec.files.flag") );

        copyAndIndexFlag =  Boolean.parseBoolean( map.get("copy.and.index.flag") );
        answerRankingFlag =  Boolean.parseBoolean( map.get("answer.ranking.flag") );
        rjCollectionFlag =  Boolean.parseBoolean( map.get("rj.collection.flag") );
        rankAnswersFLag =  Boolean.parseBoolean( map.get("rank.answers.flag") );
        yosiRankingFlag = Boolean.parseBoolean(map.get("yosi.ranking.flag"));
        mergingRequiredFlag = Boolean.parseBoolean(map.get("merging.required.flag"));
        indexMergedCollectionFlag = Boolean.parseBoolean(map.get("index.merged.collection.flag"));
        findBestGraphsFlag = Boolean.parseBoolean(map.get("find.best.graphs.flag"));
        virtualDocumentCreationFlag = Boolean.parseBoolean(map.get("virtual.documents.creation.flag"));
        virtualDocAndPruningFlag = Boolean.parseBoolean(map.get("virtual.doc.and.pruning.flag"));

        offlinePhaseFlag = Boolean.parseBoolean(map.get("offline.phase.flag"));
        onlineCommonPhaseFlag = Boolean.parseBoolean(map.get("online.common.phase.flag"));
        onlineBlancoPhaseFlag = Boolean.parseBoolean(map.get("online.blanco.phase.flag"));
        onlineYosiPhaseFlag = Boolean.parseBoolean(map.get("online.yosi.phase.flag"));
        onlineVirtualDocAndPruningFlag = Boolean.parseBoolean(map.get("online.virtual.doc.and.pruning.flag"));
        onlineVirtualDocAndPruningOnTheFlyFlag = Boolean.parseBoolean(map.get("online.virtual.doc.and.pruning.on.the.fly.flag"));
        tsaVdpBackFlag = Boolean.parseBoolean(map.get("tsa.vdp.back.flag"));
        tsaBackFlag = Boolean.parseBoolean(map.get("tsa.back.flag"));
    }

    public void executeTSAAlgorithm() throws IOException {
        this.executeTSAAlgorithm(false);
    }

    public void executeTSAAlgorithm(boolean multipleQueries) throws IOException {

        allAroundTimer.start();
        singleMethodTimer = Stopwatch.createUnstarted();

        //********** offline phase **********

        if(offlinePhaseFlag) {
            this.offlinePhase();
        }


        // ********** ONLINE PHASE - IR **********

        if(this.onlineCommonPhaseFlag) {
            System.out.print("\n***********\n\nExecuting now the on-line part of query " + this.queryId + ""
                    + " (the Blanco ranking)\n"
                    + "keyword query: \n" + query + "\n\n");

            timeWriter.write("\n***********\n\nExecuting now the on-line part of query " + this.queryId + ""
                    + " (the simple IR part)\n"
                    + "keyword query: \n" + query + "\n\n");

            this.onlineIRPhase(multipleQueries);
        }



        // ****** online phase PRUNING (VDP) *********
/*
        if(this.onlineVirtualDocAndPruningFlag) {
            System.out.print("\n***********\n\nExecuting now the on-line part of query " + this.queryId + ""
                    + " (the Pruning algorithm ranking)\n"
                    + "keyword query: \n" + query + "\n\n");

            timeWriter.write("\n***********\n\nExecuting now the on-line part of query " + this.queryId + ""
                    + " (the Pruning algorithm ranking)\n"
                    + "keyword query: \n" + query + "\n\n");

            long start = System.currentTimeMillis();
            this.onlinePhasePruning(multipleQueries);
            long end = System.currentTimeMillis() - start;
            System.out.println("time of Pruning in milliseconds: " + end);
        }


        timeWriter.write("the query " + queryId + " complexively  "
                + "required: " + allAroundTimer);
        System.out.println("the query " + queryId + " complexively  "
                + "required: " + allAroundTimer.stop());
        timeWriter.flush();
        allAroundTimer.reset();

 */
    }

    /** Execution of the offline phase of the algorithm.
     * */
    protected void offlinePhase() throws IOException {

        if(ThreadState.isOffLine()) {
            this.phase1();
        }
        if(ThreadState.isOffLine()) {
            this.phase2and3();//creation of "clusters"
        }
        if(ThreadState.isOffLine()) {
            this.phase4();
        }
        if(ThreadState.isOffLine()) {
            this.phase5();
        }
    }

    protected void onlineIRPhase(boolean multipleQueries) throws IOException {
        //old versions
        //		this.phase6(multipleQueries);
        //		this.phase7(multipleQueries);

        //performing the IR phase with merging
        this.phase6and7bis(multipleQueries);

        //		this.phase7ter(multipleQueries);
    }

    protected void phase1() {

        if(statisticsComputationFlag) {
            //phase 1: find the statistics
            if(singleMethodTimer.isRunning()) {
                singleMethodTimer.stop();
                singleMethodTimer.reset();
            }

            Stopwatch timer = Stopwatch.createStarted();

            StatisticsComputationPhase phase1 = new StatisticsComputationPhase();
            phase1.computeStatisticsUsingOnlyRDB();

            System.out.println("phase 1 ended in: " + timer.stop());
            try {
                timeWriter.write("phase 1 ended in: " + timer);
                timeWriter.newLine();
                timeWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            timer.reset();
        }
    }

    /** Clustering.
     * */
    protected void phase2and3 () {
        List<String> connectivityList = null;
        if(clusteringPhaseFlag) {
            //phase 2: compute the connectivity list (list of edges we can traverse)
            Stopwatch timer = Stopwatch.createStarted();

            ComputeTheTopKConnectivityList phase2 = new ComputeTheTopKConnectivityList();
            connectivityList = phase2.getTopKConnectivityList();

            ClusteringPhase phase3 = new ClusteringPhase();
            phase3.setClusterDirectoryPath(this.mainAlgorithmDirectory + "/clusters");

            phase3.TSAAlgorithm(connectivityList);

            System.out.println("phase 2 and 3 ended in: " + timer.stop());
            try {
                timeWriter.write("phases 2 and 3 ended in: " + timer);
                timeWriter.newLine();
                timeWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            timer.reset();
        }
    }

    protected void phase4 () {
        if(fromClusterToTRECDocumentsFlag) {
            //phase 4: convert from RDF to TREC
            Stopwatch timer = Stopwatch.createStarted();

            FromRDFGraphsToTRECDocuments phase4 = new FromRDFGraphsToTRECDocuments();
            //set the directory where to find the rdf clusters
            phase4.setGraphsMainDirectory(this.mainAlgorithmDirectory + "/clusters");
            //set the directory where to write the TREC files
            phase4.setOutputDirectory(this.mainAlgorithmDirectory + "/cluster_collection");

            phase4.convertRDFGraphsInTRECDocuments();

            System.out.println("phase 4 ended in: " + timer.stop());
            try {
                timeWriter.write("phase 4 ended in: " + timer);
                timeWriter.newLine();
                timeWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            timer.reset();
        }
    }

    protected void phase5 () throws IOException {
        if(indexDirectoryOfTRECFilesFlag) {
            //phase 5: create the index
            Stopwatch timer = Stopwatch.createStarted();

            //we need two indexes, one of unigrams and the second one of bigrams
            IndexerDirectoryOfTRECFiles phase5 = new IndexerDirectoryOfTRECFiles();

            phase5.setDirectoryToIndex(this.mainAlgorithmDirectory + "/cluster_collection");
            phase5.setIndexPath("cluster_index");
            if(ThreadState.isOffLine())
                phase5.index("unigram");

            phase5.setIndexPath("bigram_cluster_index");
            if(ThreadState.isOffLine())
                phase5.index("bigram");

            System.out.println("phase 5 ended in: " + timer.stop());
            try {
                timeWriter.write("phase 5 ended in: " + timer);
                timeWriter.newLine();
                timeWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            timer.reset();
        }
    }

/*
    protected void phase7(boolean multipleQueries) throws IOException {
        if(copyAndIndexFlag) {
            //phase 7: index the candidate collection
            System.out.println("begin phase 7 with query " + queryId + ": " + query);
            Stopwatch timer = Stopwatch.createStarted();

            CopyAndIndexSubgraphsPhase phase7 = new CopyAndIndexSubgraphsPhase();
            if(multipleQueries) {
                phase7.setQueryDirectoryPath(mainQueryDirectory + "/" + queryId);
                phase7.setResourceFilePath(mainQueryDirectory + "/" + queryId + "/IR/IR_rank.txt");
                phase7.setOutputCandidateGraphsDirectoryPath(mainQueryDirectory + "/"+ queryId + "/candidate_graphs");
                phase7.setOutputCandidateCollectionDirectoryPath(mainQueryDirectory + "/"+ queryId + "/candidate_collection");
                phase7.setOutputCollectionIndexPath(mainQueryDirectory + "/" + queryId + "/candidate_collection_index");
            }
            phase7.createCandidateCollection();

            timeWriter.write("phase 7 ended in: " + timer);
            timeWriter.newLine();
            timeWriter.flush();

            System.out.println("phase 7 ended in: " + timer.stop());
        }
    }
*/
    /** In this alternative phase to 6 and 7,
     * we combine different operations.
     * <p>
     * First, we rank the clusters and create a file .res obtained
     * by the BM25 model.
     * <p>
     * Then we look at these graphs and create a collection
     * of maximum 1000 new graphs obtained with a merging algorithm.
     * The merging algorithm simply scans the list of graphs and when a graph
     * is partially overlapped with the following graphs,
     * we merge the two.
     * <p>
     *  We create in this way a new collection of graphs,
     *  then we convert them in TREC documents and index them.
     *  <p>
     *  Finally, we use this new collection to create a new ranking with BM25.
     *  This second ranking is the output of our first pipeline.
     * @throws IOException
     * */
    protected void phase6and7bis(boolean multipleQueries) throws IOException {
        if(indexMergedCollectionFlag) {
            System.out.println("begin phase 6 with query " + queryId + ": " + query);
            //phase 6: create the first ranking of the graphs with 'plain' IR
            if(singleMethodTimer.isRunning())
                singleMethodTimer.stop();
            Stopwatch timer = Stopwatch.createStarted();

            //execute a first ranking in IR, writes the answer in a TS+IR.res file
            AnswerRankingPhase phase6 = new AnswerRankingPhase();
            phase6.setQuery(query);
            phase6.setQueryId(queryId);
            phase6.setResultDirectory(mainQueryDirectory + "/" + queryId + "/BM25");
            phase6.setIndexPath("cluster_index");

            if(DatabaseState.getState() == DatabaseState.LUBM) {
                /*in the case the database is LUBM, we add a module
                 * we only keep the graphs that
                 * contain all the keywords. We then rank them with BM25
                 * **/
                phase6.setFilteredGraphsDirectory(this.mainQueryDirectory + "/" + queryId + "/BM25/filteredGraphs" );
                phase6.setFilteredCollectionDirectory(this.mainQueryDirectory + "/" + queryId + "/BM25/filteredCollection");
                phase6.setFilteredCollectionIndexDirectory(this.mainQueryDirectory + "/" + queryId + "/BM25/filteredCollectionIndex");
                phase6.setGraphClustersDir(this.mainAlgorithmDirectory + "/clusters");

                try {
                    phase6.rankTheGraphsWithOnlyFullGraphs();
                } catch(NullPointerException e) {
					/*XXX in this case, the algorithm found no graph
					//with all the query words (strange but possible with LUBM)
					 * so we return an exception and we catch it in order to deal
					 * with this fact **/
                    //e.printStackTrace();
                    throw new NullPointerException(e.getMessage());
                }

            }
            else if (DatabaseState.getState() == DatabaseState.DEFAULT) {

                phase6.setGraphClustersDir(this.mainAlgorithmDirectory + "/clusters");
                if(ThreadState.isOnLine())
                    phase6.subgraphsRankingPhase();
            } else {
                phase6.setGraphClustersDir(this.mainAlgorithmDirectory + "/clusters");
                if(ThreadState.isOnLine())
                    phase6.subgraphsRankingPhase();
            }


            //create the merged collection
            AnswerCompressionPhase phase61 = new AnswerCompressionPhase();
            phase61.setRankingFilePath(mainQueryDirectory + "/" + queryId + "/BM25/run.txt");
            phase61.setCompressedCollectionOutputDirectoryPath(mainQueryDirectory + "/" + queryId + "/BM25/merged_graphs");
            if(multipleQueries) {
                phase61.setOriginalCollectionDirectoryPath(this.mainAlgorithmDirectory + "/clusters");
            }
            if(ThreadState.isOnLine())
                phase61.compressTheCollectionWithThreshold();

            //convert the new collection of graphs in documents and index them
            FromRDFGraphsToTRECDocuments phase7bis = new FromRDFGraphsToTRECDocuments();

            if(multipleQueries) {
                phase7bis.setGraphsMainDirectory(mainQueryDirectory + "/" + queryId + "/BM25/merged_graphs");
                phase7bis.setOutputDirectory(mainQueryDirectory + "/" + queryId + "/BM25/merged_collection");
            }
            if(ThreadState.isOnLine()) {
                phase7bis.setGraphsMainDirectory(mainQueryDirectory + "/" + queryId + "/BM25/merged_graphs");
                phase7bis.setOutputDirectory(mainQueryDirectory + "/" + queryId + "/BM25/merged_collection");
                phase7bis.convertRDFGraphsInTRECDocuments();
            }
            //now we index them - we perform the double indexing because
            //it will be useful for other methods and it is quick
            IndexerDirectoryOfTRECFiles phase72bis = new IndexerDirectoryOfTRECFiles();
            if(multipleQueries) {
                phase72bis.setDirectoryToIndex(mainQueryDirectory + "/" + queryId + "/BM25/merged_collection");
                phase72bis.setIndexPath(mainQueryDirectory + "/" + queryId + "/BM25/merged_unigram_index");
            }
            if(ThreadState.isOnLine()) {
                phase72bis.setDirectoryToIndex(mainQueryDirectory + "/" + queryId + "/BM25/merged_collection");
                phase72bis.setIndexPath("queries" + "/" + queryId + "/BM25/merged_unigram_index");
                phase72bis.index("unigram");
            }
            if(multipleQueries) {
                phase72bis.setIndexPath(mainQueryDirectory + "/" + queryId + "/BM25/merged_bigram_index");
            }
            if(ThreadState.isOnLine()) {
                phase72bis.setIndexPath("queries" + "/" + queryId + "/BM25/merged_bigram_index");
                phase72bis.index("bigram");
            }

            //rank the new collection
            if(multipleQueries) {
                phase6.setQuery(query);
                phase6.setIndexPath(mainQueryDirectory + "/" + queryId + "/BM25/merged_unigram_index");
                phase6.setResultDirectory(mainQueryDirectory + "/" + queryId + "/BM25/answer");
                phase6.setQueryId(queryId);
            }
            if(ThreadState.isOnLine()) {
                // uses the same query as before
                phase6.setResultDirectory(mainQueryDirectory + "/" + queryId + "/BM25/BM25");
                phase6.setIndexPath("queries" + "/" + queryId + "/BM25/merged_unigram_index");
                phase6.subgraphsRankingPhase();
            }

            System.out.println("TSA+BM25 phase ended in: " + timer.stop());

            timeWriter.write("TSA+BM25 phase ended in: " + timer);
            timeWriter.newLine();
            timeWriter.flush();
        }
    }

    /** Test main (if I rename it 'test' is for debugging reasons)
     * */
    public static void main(String[] args) throws IOException {
        TSAMainClass execution = new TSAMainClass();
        if(execution.isMultipleQueries()) {
            //execution.executeTSAAlgorithmMultipleTimes();
        }
        else {
            execution.executeTSAAlgorithm();
        }
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public boolean isMultipleQueries() {
        return multipleQueries;
    }

    public void setMultipleQueries(boolean multipleQueries) {
        this.multipleQueries = multipleQueries;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }
}
