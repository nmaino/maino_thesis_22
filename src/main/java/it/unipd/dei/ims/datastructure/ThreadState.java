package it.unipd.dei.ims.datastructure;

/** This class only contains two static fields that
 * represents the information if a thread have the
 * authorization to continue its execution or to stop.
 * */
public class ThreadState {

    /** Set this to true if we have the authorization
     * to be still executing on-line.
     * */
    static boolean onLine = true;

    /** Set this to true if we have
     * the authorization to be still executing off-line.
     *  */
    static boolean offLine = true;


    /** returns true if we have the authorization to continue
     * to execute on-line
     * */
    public static boolean isOnLine() {
        return onLine;
    }

    public static void setOnLine(boolean o) {
        onLine = o;
    }

    /** returns true if we have the authorization to continue
     * to execute off-line
     * */
    public static boolean isOffLine() {
        return offLine;
    }

    public static void setOffLine(boolean o) {
        offLine = o;
    }



}
