package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private int counter;
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	if(!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc()))
    		throw new DbException("Tuples' type don't match");
    	this.t=t;
    	this.child=child;
    	this.tableId=tableId;
    	this.td=new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of inserted tuples"});
    	this.called=false;
    	this.counter=-1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.counter=0;
    	this.child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	this.child.close();
    	super.close();
    	this.counter=-1;
    	this.called=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	this.counter=0;
    	this.called=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(this.called)
    		return null;
    	this.called=true;
    	while (this.child.hasNext()) {
			Tuple tuple=child.next();
			try {
				Database.getBufferPool().insertTuple(t, tableId, tuple);
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
