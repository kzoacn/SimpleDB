package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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

    int gbfield; Type gbfieldtype; int afield;Op what;
    
    HashMap<Field, Integer> sum,max,min,cnt,avg;
    HashMap<Field, Tuple>ans;
    
    TupleDesc tupleDesc;
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
    	sum=new HashMap<Field, Integer>();
    	max=new HashMap<Field, Integer>();
    	min=new HashMap<Field, Integer>();
    	cnt=new HashMap<Field, Integer>();
    	avg=new HashMap<Field, Integer>();
    	
    	ans=new HashMap<Field, Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    private void touch(Field key,int val) {
		if(!sum.containsKey(key))
			sum.put(key, 0);
		if(!max.containsKey(key))
			max.put(key, val);
		if(!min.containsKey(key))
			min.put(key, val);
		if(!cnt.containsKey(key))
			cnt.put(key, 0);
		if(!avg.containsKey(key))
			avg.put(key, 0);
	}
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key=null;
    	if(gbfield == NO_GROUPING) {
    		
    	}else {
    		key=tup.getField(gbfield);
    	}
    	int val=((IntField)tup.getField(afield)).getValue();
    	touch(key,val);
    	sum.put(key,sum.get(key)+val);
    	max.put(key, Math.max(max.get(key),val));
    	min.put(key, Math.min(min.get(key),val));
    	cnt.put(key,cnt.get(key)+1);
    	avg.put(key,sum.get(key)/cnt.get(key));
    	
    	if(gbfield==NO_GROUPING) {
    		Tuple tuple=new Tuple(tupleDesc);
        	switch (what) {
    		case SUM:
    			tuple.setField(0, new IntField(sum.get(key)));
    			break;
    		case MAX:
    			tuple.setField(0, new IntField(max.get(key)));
    			break;
    		case MIN:
    			tuple.setField(0, new IntField(min.get(key)));
    			break;
    		case AVG:
    			tuple.setField(0, new IntField(avg.get(key)));
    			break;
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
    		case SUM:
    			tuple.setField(1, new IntField(sum.get(key)));
    			break;
    		case MAX:
    			tuple.setField(1, new IntField(max.get(key)));
    			break;
    		case MIN:
    			tuple.setField(1, new IntField(min.get(key)));
    			break;
    		case AVG:
    			tuple.setField(1, new IntField(avg.get(key)));
    			break;
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
