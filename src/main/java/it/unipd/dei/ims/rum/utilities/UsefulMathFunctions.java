package it.unipd.dei.ims.rum.utilities;

public class UsefulMathFunctions {

	public static int subDirectoryIndexForClusters(int id) {
		
		int offset = (int) Math.ceil((double) id / 2048);
		return offset;
	}
	
	
	/** Computes the freaking log_2 of some argument*/
	public static double logBase2Of(double argument) {
		//added an epsilon in order to avoid miscalculation fue to floating point imprecisions
		//just to be sure (suggested by stackoverflow, keep under observation)
		return (double) (Math.log(argument) / Math.log(2) + 1e-10);
	}
}
