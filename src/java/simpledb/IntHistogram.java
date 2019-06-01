package simpledb;


/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
	int buckets;
	int min,max,size,ntups;
	int[] buck;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets=buckets;
    	this.min=min;
    	this.max=max;
    	this.ntups=0;
    	this.size=(max-min+1+buckets-1)/buckets;
    	buck=new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int id =  (v - min) / size;
        buck[id]++;
        ntups++;
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
    	
    	if(ntups==0)
    		return 0;
    	int id;
    	double  ans;
    	
    	id = (v - min) / size;
        ans = 0.0;
        
    	switch (op) {
    	case EQUALS:
            if (v < min || v > max)
                return 0.0;
            return 1.0 * buck[id] / size / ntups;
    	case GREATER_THAN:
            if (v < min)
                return 1.0;
            if (v > max)
                return 0.0;
            for (int i = id + 1; i < buckets; i++)
                ans += 1.0 * buck[i] / ntups;
            return ans;
		case GREATER_THAN_OR_EQ:
            if (v < min)
                return 1.0;
            if (v > max)
                return 0.0;
            for (int i = id ; i < buckets; i++)
                ans += 1.0 * buck[i] / ntups;
            return ans;
		case LESS_THAN:                
			if (v < min)
	            return 0.0;
	        if (v > max)
	            return 1.0;
	        for (int i = 0; i < id; i++)
	            ans += 1.0 * buck[i] / ntups;
	        return ans;
		case LESS_THAN_OR_EQ:                
			if (v < min)
	            return 0.0;
	        if (v > max)
	            return 1.0;
	        for (int i = 0; i <= id; i++)
	            ans += 1.0 * buck[i] / ntups;
	        return ans;
		case NOT_EQUALS:
            if (v < min || v > max)
                return 1.0;
            return 1-1.0 * buck[id] / size / ntups;
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
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
