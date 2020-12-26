package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private int counter;
    private boolean called;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.t=t;
    	this.child=child;
    	this.td=new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of deleted tuples"});
    	this.called=false;
    	this.counter=-1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.open();
    	super.open();
    	this.counter=0;
    }

    public void close() {
        // some code goes here
    	super.close();
    	this.child.close();
    	this.counter=-1;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	this.counter=0;
    	
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(this.called)
    		return null;
    	this.called=true;
    	while (this.child.hasNext()) {
			Tuple tuple=child.next();
			try {
				Database.getBufferPool().deleteTuple(t, tuple);
				++this.counter;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
		}
    	Tuple result=new Tuple(td);
    	result.setField(0, new IntField(counter));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	 return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.child=children[0];
    }

}
