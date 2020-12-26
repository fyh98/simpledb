package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;



/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield=gbfield;
    	this.afield=afield;
    	this.gbfieldtype=gbfieldtype;
    	this.what=what;
    	groupMap=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	StringField agField=(StringField)tup.getField(afield);
    	String agValue=agField.getValue();
    	Field gbField= this.gbfield==NO_GROUPING? null : tup.getField(gbfield);
    	if(groupMap.get(gbField)!=null) {
			int preValue=groupMap.get(gbField);
			groupMap.put(gbField, preValue+1);
		}
		else {
			groupMap.put(gbField, 1);
		}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new AggregateIterator(this.groupMap, this.gbfieldtype);
    }
   
}
 class AggregateIterator implements OpIterator {
	protected Iterator<Map.Entry<Field, Integer>> it;
	TupleDesc td;
	private Map<Field, Integer> groupMap;
	protected Type itgbfieldtype;
	public AggregateIterator(Map<Field, Integer> groupMap,Type itgbfieldType) {
		this.groupMap=groupMap;
		this.itgbfieldtype=itgbfieldType;
		if(this.itgbfieldtype==null) {
			this.td=new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
		}
		else {
			this.td=new TupleDesc(new Type[] {this.itgbfieldtype , Type.INT_TYPE},new String[] {"groupVal","aggregateVal"} );
		}
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		this.it=groupMap.entrySet().iterator();
	}
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return it.hasNext();
	}
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		Map.Entry<Field, Integer> entry=this.it.next();
		Field field=entry.getKey();
		Tuple tuple=new Tuple(td);
		this.setFields(tuple, entry.getValue(), field);
		return tuple;
	}
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		this.it=groupMap.entrySet().iterator();
	}
	@Override
	public TupleDesc getTupleDesc() {
		// TODO Auto-generated method stub
		return td;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		this.it=null;
		this.td=null;
	}
	void setFields(Tuple rtn,int value,Field f) {
		if(f==null) {
			rtn.setField(0, new IntField(value));
		}
		else {
			rtn.setField(0, f);
			rtn.setField(1, new IntField(value));
		}
	}
}