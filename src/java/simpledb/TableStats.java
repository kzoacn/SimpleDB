package simpledb;

import java.sql.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    
    int tableid;
    int ioCostPerPage;
    boolean[] isInt;
    IntHistogram[] intHistograms;
    StringHistogram[] stringHistograms;
    int tupleNum;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.tableid=tableid;
    	this.ioCostPerPage=ioCostPerPage;
    	DbFile file=Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc td = file.getTupleDesc();
        int numFields = td.numFields();
        isInt = new boolean[numFields];
        this.intHistograms = new IntHistogram[numFields];
        this.stringHistograms = new StringHistogram[numFields];
        int[] min = new int[numFields];
        int[] max = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                isInt[i] = true;
                min[i] = Integer.MAX_VALUE;
                max[i] = Integer.MIN_VALUE;
            }
            else
            	isInt[i] = false;
        }
        tupleNum = 0;
        TransactionId tid=new TransactionId();
        try {
            DbFileIterator it = file.iterator(tid);
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                this.tupleNum++;
                for (int i = 0; i < numFields; i++) {
                    Field f = t.getField(i);
                    if (isInt[i]) {
                        int val = ((IntField)f).getValue();
                        min[i]=Math.min(min[i], val);
                        max[i]=Math.max(max[i], val);
                    }
                }
            }
        } catch(Exception e) {}
        
        for (int i = 0; i < numFields; i++) {
            if (isInt[i])
                intHistograms[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);    
            else
                stringHistograms[i] = new StringHistogram(NUM_HIST_BINS);
        }
        try {
        	DbFileIterator it = file.iterator(tid);
            it.open();
        	while (it.hasNext()) {
                Tuple t = it.next();
                for (int i = 0; i < numFields; i++) {
                    Field f = t.getField(i);
                    if (isInt[i]) {
                        int val = ((IntField)f).getValue();
                        intHistograms[i].addValue(val);
                    }
                    else {
                        String val = ((StringField)f).getValue();
                        stringHistograms[i].addValue(val);
                    }
                }
            }
        } catch(Exception e) {}

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        DbFile file=Database.getCatalog().getDatabaseFile(tableid);
        if(file instanceof HeapFile)
        	return 1.0*((HeapFile)file).numPages()*ioCostPerPage;
        if(file instanceof BTreeFile)
        	return 1.0*((BTreeFile)file).numPages()*ioCostPerPage;
        return ioCostPerPage*2.33;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	if(field<0 || field>=isInt.length)
    		return 0;
    	if(isInt[field])
    		return intHistograms[field].estimateSelectivity(op, ((IntField)constant).getValue());
    	else
    		return stringHistograms[field].estimateSelectivity(op, ((StringField)constant).getValue());
        
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}