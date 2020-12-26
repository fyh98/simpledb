package simpledb;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;





/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    private Map<Field, Integer> groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield=gbfield;
    	this.afield=afield;
    	this.gbfieldtype=gbfieldtype;
    	this.what=what;
    	groupMap=new HashMap<>();
    	countMap=new HashMap<>();
    	avgMap=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	IntField agField=(IntField)tup.getField(afield);
    	int agValue=agField.getValue();
    	Field gbField= this.gbfield==NO_GROUPING? null : tup.getField(gbfield);
    	if(gbField!=null&&gbField.getType()!=this.gbfieldtype)
    		throw new IllegalArgumentException("the tuple's type is wrong");
    	switch (this.what) {
		case MIN:
			if(groupMap.get(gbField)!=null) {
				if(agValue<groupMap.get(gbField))
					groupMap.put(gbField, agValue);
			}
			else {
				groupMap.put(gbField, agValue);
			}
			break;
		case MAX:
			if(groupMap.get(gbField)!=null) {
				if(agValue>groupMap.get(gbField))
					groupMap.put(gbField, agValue);
			}
			else {
				groupMap.put(gbField, agValue);
			}
			break;
		case SUM:
			if(groupMap.get(gbField)!=null) {
				int preValue=groupMap.get(gbField);
					groupMap.put(gbField, agValue+preValue);
			}
			else {
				groupMap.put(gbField, agValue);
			}
			break;
		case SUM_COUNT:
		case AVG:
			if(avgMap.get(gbField)!=null) {
				List<Integer> list=avgMap.get(gbField);
				list.add(agValue);
			}
			else {
				List<Integer> list=new ArrayList<>();
				list.add(agValue);
				avgMap.put(gbField, list);
			}
			break;
		case COUNT:
			if(groupMap.get(gbField)!=null) {
				int preValue=groupMap.get(gbField);
				groupMap.put(gbField, preValue+1);
			}
			else {
				groupMap.put(gbField, 1);
			}
			break;
		case SC_AVG:
			IntField countField;
			if(gbField==null) {
				countField=(IntField)tup.getField(1);
			}
			else {
				countField=(IntField)tup.getField(2);
			}
			int countValue=countField.getValue();
			if(this.groupMap.get(gbField)!=null) {
				this.groupMap.put(gbField, agValue);
				this.countMap.put(gbField, countValue);
			}
			else {
				int preAgValue=this.groupMap.get(gbField);
				int preCountValue=this.groupMap.get(gbField);
				this.groupMap.put(gbField, agValue+preAgValue);
				this.countMap.put(gbField, countValue+preCountValue);
			}
			break;
		default:
			System.out.println("not support");
			break;
		}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggIterator();
    }
    private class IntAggIterator extends AggregateIterator{
    	private Iterator<Map.Entry<Field, List<Integer>>> avgIt;
    	private boolean isAVG;
    	private boolean isScAVG;
    	private boolean isSumCount;
    	 IntAggIterator() {
			// TODO Auto-generated constructor stub
    		super(groupMap, gbfieldtype);
    		this.isAVG=what.equals(Op.AVG);
    		this.isScAVG=what.equals(Op.SC_AVG);
    		this.isSumCount=what.equals(Op.SUM_COUNT);
    		if(isSumCount) {
    			this.td=new TupleDesc(new Type[] {this.itgbfieldtype,Type.INT_TYPE , Type.INT_TYPE},new String[] {"groupVal","sumVal","countVal"} );
    		}
		}
    	@Override
    	public void open() throws DbException, TransactionAbortedException {
    		// TODO Auto-generated method stub
    		super.open();
    		if(this.isAVG||this.isSumCount) {
    			this.avgIt=avgMap.entrySet().iterator();
    		}
    		else {
    			this.avgIt=null;
    		}
    	}
    	@Override
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		// TODO Auto-generated method stub
    		if(this.isAVG||this.isSumCount) {
    			return avgIt.hasNext();
    		}
    		else {
    			return super.hasNext();
    		}
    	}
    	@Override
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		// TODO Auto-generated method stub
    		Tuple tuple=new Tuple(td);
    		if(this.isAVG||this.isSumCount) {
    			Map.Entry<Field, List<Integer>> avgSumCount=this.avgIt.next();
    			Field fieldAvgSumCount=avgSumCount.getKey();
    			List<Integer> avgSumCountList=avgSumCount.getValue();
    			if(this.isAVG) {
    				int value=sum(avgSumCountList)/avgSumCountList.size();
    				this.setFields(tuple, value, fieldAvgSumCount);
    				return tuple;
    			}
    			else {
    				int value=sum(avgSumCountList);
					this.setFields(tuple, value, fieldAvgSumCount);
					if(fieldAvgSumCount==null) {
						tuple.setField(1, new IntField(avgSumCountList.size()));
					}
					else {
						tuple.setField(2, new IntField(avgSumCountList.size()));
					}
					return tuple;
				}
    		}
    		else {
    			if(this.isScAVG) {
    				Map.Entry<Field, Integer> scAvg=this.it.next();
    				Field fieldScAvg=scAvg.getKey();
    				int value=scAvg.getValue();
    				this.setFields(tuple, value/countMap.get(fieldScAvg), fieldScAvg);
    				return tuple;
    			}
    		}
    		return super.next();
    	}
    	private int sum(List<Integer> l) {
			int sum=0;
			for(int e:l)
				sum+=e;
			return sum;
		}
    	@Override
    	public void close() {
    		// TODO Auto-generated method stub
    		super.close();
    		this.it=null;
    	}
    }
}

