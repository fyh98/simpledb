package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int[] buckets;
	private int min;
	private int max;
	private int ntups;
	private double width;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets=new int[buckets];
    	this.min=min;
    	this.max=max;
    	this.width=((double)(max-min+1))/buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	if(v<min||v>max)
    		throw new IllegalArgumentException("value out of range");
    	int index=(int)((v-min)/width);
    	++buckets[index];
    	++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	switch (op) {
		case LESS_THAN:
			if(v<=min)
				return 0.0;
			if(v>=max)
				return 1.0;
			int index=(int)((v-min)/width);
			double count=0;
			for(int i=0;i<index;++i) {
				count+=buckets[i];
			}
			count+=(buckets[index]/width)*(v-index*width-min);
			return count/ntups;
		case LESS_THAN_OR_EQ:
			return estimateSelectivity(Op.LESS_THAN, v+1);
		case GREATER_THAN:
			return 1-estimateSelectivity(Op.LESS_THAN_OR_EQ, v);
		case GREATER_THAN_OR_EQ:
			return estimateSelectivity(op.GREATER_THAN, v-1);
		case EQUALS:
			return estimateSelectivity(Op.LESS_THAN_OR_EQ, v)-estimateSelectivity(Op.LESS_THAN, v);
		case NOT_EQUALS:
			return 1-estimateSelectivity(Op.EQUALS, v);
		default:
			break;
		}
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
    	int count=0;
    	for(int i=0;i<buckets.length;++i)
    		count+=buckets[i];
    	System.out.println(((double)count)/ntups);
        return ((double)count)/ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "length:"+buckets.length+" min: "+min+" max:"+max;
    }
}
