package simpledb;

import java.io.IOException;
import java.lang.Exception;

/** Exception that is thrown when a transaction has aborted. */
public class TransactionAbortedException extends Exception {
    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    public TransactionAbortedException() {
    	
    }
    public TransactionAbortedException(TransactionId tId) throws IOException {
    	//System.out.println(tId+" aborted");
    	//Database.getBufferPool().transactionComplete(tId, false);
    	this.tId=tId;
    }
    public TransactionId getTid() {
    	return tId;
    }
}
