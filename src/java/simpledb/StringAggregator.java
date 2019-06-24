package simpledb;

import java.util.HashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    int gbfield; Type gbfieldtype; int afield;Op what;
    
    HashMap<Field, Integer> sum,max,min,cnt,avg;
    HashMap<Field, Tuple>ans;
    
    TupleDesc tupleDesc;
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
        if (this.gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    	cnt=new HashMap<Field, Integer>();
    	
    	ans=new HashMap<Field, Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    private void touch(Field key) {
		if(!cnt.containsKey(key))
			cnt.put(key, 0);
	}
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key=null;
    	if(gbfield == NO_GROUPING) {
    		
    	}else {
    		key=tup.getField(gbfield);
    	}
    	touch(key);
    	cnt.put(key,cnt.get(key)+1);
    	
    	if(gbfield==NO_GROUPING) {
    		Tuple tuple=new Tuple(tupleDesc);
        	switch (what) {
    		case COUNT:
    			tuple.setField(0, new IntField(cnt.get(key)));
    			break;
    		default:
    			break;
    		}
			ans.put(key,tuple);
    	}else {
    		Tuple tuple=new Tuple(tupleDesc);
			tuple.setField(0, key);
        	switch (what) {
    		case COUNT:
    			tuple.setField(1, new IntField(cnt.get(key)));
    			break;
    		default:
    			break;
    		}
			ans.put(key,tuple);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(tupleDesc, ans.values());
    }

}
