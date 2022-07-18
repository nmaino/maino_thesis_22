package it.unipd.dei.ims.tsa.main;

import it.unipd.dei.ims.datastructure.ConnectionHandler;
import it.unipd.dei.ims.datastructure.DatabaseState;
import it.unipd.dei.ims.datastructure.ThreadState;
import it.unipd.dei.ims.rum.utilities.PropertiesUsefulMethods;
import org.apache.jena.ext.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

/** This class controls all the execution of the 
 * pipeline based on TSA.
 * <ul>
 * <li>TSA + IR
 * <li>TSA + SML
 * <li>TSA + MRF-KS
 * <li>TSA + VDP
 * </ul>
 * 
 * For practical reasons, for now I have only produced the first and last methods.
 * This class is similar to the {@link TSAMainClass}, but it differs from it
 * in the sense that I added a way to interrupt executions off-line.
 * 
 * 
 * */
public class TSAControlRoomWithTimeouts extends TSAMainClass {
	
	
	/** set to true if we require the execution of the off-line 
	 * part of the TSA algorithm. 
	 * NB: the executions of the pipeline SML and MRF-KS
	 * presuppose that their off-line phases have already been
	 * executed some other time. 
	 * <p>
	 * In this regard, look at the main classes of those projects
	 * */
	private boolean isOffline = true;
	
	/** provided time for the off-line phase in hours*/
	private int offLineTime;

	/** provided time for the on-line time phase in minutes*/
	private int onLineTime;
	
	
	/**builder of the class. It takes care to set all the parameters
	 * reading them from the properties/main.properties
	 * file
	 * 
	 * 
	 * */
	public TSAControlRoomWithTimeouts() {
		//the super constructor takes all the information
		//from the main.properties file and initializes all
		//the useful fields with them
		super();
		
		this.mainQueryDirectory = this.mainAlgorithmDirectory + "/queries";
		
		String size_ = map.get("database.size");
		size_ = (size_!= null) ? size_ : "small";
		if(size_.equals("small"))
			DatabaseState.setSize(DatabaseState.SMALL);
		else if(size_.equals("big"))
			DatabaseState.setSize(DatabaseState.BIG);
		
		this.isOffline = Boolean.parseBoolean(this.getMap().get("is.offline"));
		
		//set the kind of database we are using.
		//this is necessary in order to correctly translate graphs into documents
		String dbName = map.get("rdf.database.name");
		DatabaseState.setState(dbName);
		
		String time = map.get("on.line.time");
		time = (time != null) ? time : "1000";
		this.onLineTime = Integer.parseInt(time);
		
		time = map.get("off.line.time");
		time = (time != null) ? time : "48";
		this.offLineTime = Integer.parseInt(time);
	}
	
	/** Execute the off-line phase of the TSA algorithm.
	 * */
	public void offLinePhaseWithTimer() {
		
		
		//we create an ExecutorService to deal with this thing
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<String> future = null;
		
		try {
			//setup the connection to the database once and for all, we will close it 
			//at the end of the execution
			DriverManager.getConnection(this.jdbcConnectionString, username, password);
			
			//now create a Future object to check our thread
			future = executor.submit(new OfflineTask(this));
			
			//give the execution 24h of time to complete
//			System.out.println(future.get(1, TimeUnit.SECONDS));
			String s = future.get(this.offLineTime, TimeUnit.HOURS);
			System.out.println(s);
			timeWriter.write(s);
			timeWriter.newLine();
			timeWriter.flush();
			timeWriter.write("\n ---------- END EXECUTION ----------\n");
			timeWriter.close();
			
		} catch (TimeoutException e) {
			future.cancel(true);
			ThreadState.setOffLine(false);
			System.out.println("the off-line phase of TSA could not terminate in time!");
		} catch (InterruptedException e) {
			future.cancel(true);
			ThreadState.setOffLine(false);
			System.out.println("there was an InterruptedException during the off-line"
					+ " phase of TSA");
			e.printStackTrace();
		} catch (ExecutionException e) {
			future.cancel(true);
			ThreadState.setOffLine(false);
			System.out.println("there was an ExecutionException during the off-line phase of TSA");
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
			} catch (SQLException e) {
				System.err.println("error in closing the connection at the end of the off-line phase");
				e.printStackTrace();
			}
		}
		
		executor.shutdownNow();
	}
	
	/** This task represent the thread that will attempt the off-line phase 
	 * of TSA.
	 * */
	private class OfflineTask implements Callable<String> {
		/**link to the current execution*/
		private TSAControlRoomWithTimeouts execution;
		
		/**Constructor to get the reference to our current execution
		 * with all the data we need.
		 * */
		public OfflineTask(TSAControlRoomWithTimeouts exec) {
			this.execution = exec;
		}
		
		@Override
		public String call() throws Exception {
			//here we execute our off-line computations
			System.out.println("starting the execution of the off-line phase");
			Stopwatch offFlineTimer = Stopwatch.createStarted();
			//simply call the offlinePhase() method that we have in the MainClass
			this.execution.offlinePhase();
			return "TIME off-line " + offFlineTimer.stop();
		}
	}
	
	/** This method execute the on-line phase, executing 
	 * once per query provided in the queries.properties
	 * file.
	 * 
	 * In particular, the method performs 4 different phases, one for each 
	 * pipeline.
	 * */
	protected void onlinePhaseWithTimer() {
		//first of all, get the list of queries
		String queryPath = this.map.get("query.file");
		try {
			Map<String, String> queryMap = PropertiesUsefulMethods.getSinglePropertyFileMap(queryPath);
			
			//setup the connection to the database once and for all, we will close it 
			//at the end of the execution
			DriverManager.getConnection(this.jdbcConnectionString, username, password);
			
			//get this little number - currently I do not need it, so who cares
//			this.totalDegree = this.getTotalDegreeForYosi();
			
			
			//now for each query
			for(Entry<String, String> entry : queryMap.entrySet()) {
				ThreadState.setOnLine(true);
				
				//set the information of this query
				String queryId = entry.getKey();
				String query = entry.getValue();
				this.setQueryId(queryId);
				this.setQuery(query);
				
				this.executeOnLinePhaseWithTimer();
			}
			timeWriter.write("\n ---------- END EXECUTION ----------\n");
			timeWriter.close();
			
		} catch (IOException e) {
			System.err.println("Unable to read the query file " + queryPath);
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("Unable to connect to the database "
					+ "with the jdbc driver: " + map.get("jdbc.connection.string"));
			e.printStackTrace();
		} finally {
			try {
				ConnectionHandler.closeConnectionIfOwner(this.getClass().getName());
			} catch (SQLException e) {
				System.err.println("error closing the connection to the database");
				e.printStackTrace();
			}
		}
		
	}
	
	/** This method executes one query at a time, 
	 * one on-line phase per time. It makes sure that every phase is 
	 * executed inside a maximum time of 1000s or the 
	 * ones provided in the on.line property
	 * */
	protected void executeOnLinePhaseWithTimer() {
		//first pipeline is the TSA+BM25
		if(this.indexMergedCollectionFlag && ThreadState.isOnLine())
			this.executeBM25PhaseWithTimer();
		
		//NOT IMPLEMENTED YET
		//second pipeline is the TSA+SLM
//		this.executeSLMPhaseWithTimer();
		//third pipeline is the TSA+MRF-KS
//		this.executeMRFKSPhaseWithTimer();
		
		
		//fourth pipeline is the TSA+VDP (various options)
		if(this.onlineVirtualDocAndPruningOnTheFlyFlag ||
				this.tsaVdpBackFlag ||
				this.tsaBackFlag)
			this.executeVDPPhaseWithTimer();
	}
	
	/**Executes one query with the on-line phase of the
	 * TSA+BM25 pipeline
	 * 
	 * */
	protected void executeBM25PhaseWithTimer() {
		//create the thread that will take care of this query
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<String> future = executor.submit(new BM25PhaseTask(this));

		try {
			//execute the operation and report the time used
			String s = future.get(this.onLineTime, TimeUnit.SECONDS);
			System.out.println(s);
			timeWriter.write(s);
		} catch (TimeoutException e) {
			future.cancel(true);
			ThreadState.setOnLine(false);
			System.out.println("TIME TSA+BM25 " + this.queryId + " -1");
		} catch (InterruptedException e) {
			future.cancel(true);
			ThreadState.setOnLine(false);
			System.out.println("there was an InterruptedException during the TSA+BM25 pipeline");
			e.printStackTrace();
		} catch (ExecutionException e) {
			future.cancel(true);
			ThreadState.setOnLine(false);
			System.out.println("there was an ExecutionException during the TSA+BM25 pipeline");
			e.printStackTrace();
		} catch (IOException e) {
			future.cancel(true);
			ThreadState.setOnLine(false);
			System.err.println("error with the writer in the BM25PhaseTask");
			e.printStackTrace();
		}
		
		executor.shutdownNow();
		
	}
	
	private class BM25PhaseTask implements Callable<String> {
		/**link to the current execution*/
		private TSAControlRoomWithTimeouts execution;
		
		/**Constructor to get the reference to our current execution
		 * with all the data we need.
		 * */
		public BM25PhaseTask(TSAControlRoomWithTimeouts exec) {
			this.execution = exec;
		}
		
		@Override
		public String call() throws Exception {
			System.out.println("starting the execution of the TSA+BM25 pipeline with timer for query " + queryId);
			Stopwatch bm25Timer = Stopwatch.createStarted();
			/*the boolean true is a legacy from previous
			 * implementations. It is necessary in order
			 * for the method to set the new query and queryId
			 * */
			try {
				this.execution.phase6and7bis(true);//simply call the method of the parent class
				return "TIME TSA+BM25 " + this.execution.queryId + " " + bm25Timer.stop();
			} catch (NullPointerException e) {
				return "TIME TSA+BM25 " + this.execution.queryId +  " " + bm25Timer.stop() + " -1 (but no documents were produced)";
			}
		}
	}
	

	/** This method executes the VDP part of the TSA+VDP part of the keyword search algorithm
	 (and in particular the part after the ranking with BM25 and the merging)
	 
	 This is simply the method which starts the execution. There is a class Task
	 which actually manages to invoke the correct method depending on the
	 flags chosen in the property files and executes the computation. 
	 <p>
	 Moreover this instance of the algorithm uses a timer, thus we have a time limit in our execution.
	 The main thread invokes the correct methods and waits for the given amount of
	 time for the completion of the algorithm.
	 Note that the second thread, when the time stops, won't stop by itself.
	 You need to deal with what happens when the time runs out.
	 
	 */
	protected void executeVDPPhaseWithTimer() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<String> future = executor.submit(new VDPPipelineTask(this));

		try {
			String s = future.get(this.onLineTime, TimeUnit.SECONDS);
			System.out.println(s);
			timeWriter.write(s);
			timeWriter.newLine();
			timeWriter.flush();
		} catch (TimeoutException e) {
			future.cancel(true);
			System.out.println("the TSA+VDP pipeline could not terminate in time!");
			System.out.println("TIME TSA+VDP " + this.queryId + " -1");
		} catch (InterruptedException e) {
			future.cancel(true);
			System.out.println("there was an InterruptedException during the TSA+VDP pipeline");
			e.printStackTrace();
		} catch (ExecutionException e) {
			future.cancel(true);
			System.out.println("there was an ExecutionException during the TSA+VDP pipeline");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("problems with the writer in the VDPPipeline Task");
			e.printStackTrace();
		}
		
		executor.shutdownNow();
	}
	
	/** Task containing all the pipelines for the different variants
	 * of TSA+VDP. It implements the thread that executes in parallel*/
	private class VDPPipelineTask implements Callable<String> {
		/**link to the current execution*/
		private TSAControlRoomWithTimeouts execution;
		
		/**Constructor to get the reference to our current execution
		 * with all the data we need.
		 * */
		public VDPPipelineTask(TSAControlRoomWithTimeouts exec) {
			this.execution = exec;
		}
		
		@Override
		public String call() throws Exception {
			System.out.println("starting the execution of the TSA+VDP pipeline with timer");
			Stopwatch timer = Stopwatch.createStarted();
			/*the boolean true is a legacy from previous
			 * implementations. It is necessary in order
			 * for the method to set the new query and queryId
			 * 
			 * phase11bis is simply the TSA+VDP pipeline,
			 * implemented in the super class.
			 * */
			try {
				//vanilla TSA+VDP (never thought I would call VDP 'vanilla')
				if(this.execution.onlineVirtualDocAndPruningOnTheFlyFlag) {
					this.execution.phase11bis(true);//simply call the method of the parent class
					return "TIME TSA+VDP " + this.execution.queryId + " " + timer.stop();					
				} /*else if(this.execution.tsaVdpBackFlag) {
					//TSA+VDP+BACK
					this.execution.tsaVdpBackPhase();
					return "TIME TSA+VDP+BACK " + this.execution.queryId + " " + timer.stop();
				} else if(this.execution.tsaBackFlag) {
					//TSA+BACK
					this.execution.tsaBackPhase();
					return "TIME TSA+BACK " + this.execution.queryId + " " + timer.stop();
				} */
				return "";
				
			} catch (IllegalStateException e) {
				return "TIME TSA+VDP " + this.execution.queryId +  " " + timer.stop() + " -1 no document found";
			}
		}
	}
	
	
	/** This main is the starting point of all the project.
	 * With the parameters in the file main.properties you can
	 * command the execution of this class in its various forms.
	 * 
	 * */
	public static void main(String[] args) {
		
//		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//		loggers.add(LogManager.getRootLogger());
//		for ( Logger logger : loggers ) {
//		    logger.setLevel(Level.OFF);
//		}
//		
//		Logger logger = Logger.getLogger("org.openrdf.model.impl");
//		logger.setLevel(Level.WARN);
		
		
		TSAControlRoomWithTimeouts execution = new TSAControlRoomWithTimeouts();
		
		if(execution.isOffline()) {
			//execute the off-line phase 
			execution.offLinePhaseWithTimer();
		} else {
			//execute the online phase of the algorithm
			execution.onlinePhaseWithTimer();
		}
	}


	public boolean isOffline() {
		return isOffline;
	}


	public void setOffline(boolean isOffline) {
		this.isOffline = isOffline;
	}

}
