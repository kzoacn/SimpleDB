package simpledb;

import static org.junit.Assert.fail;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.sun.media.sound.RIFFInvalidDataException;

import sun.management.counter.Variability;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    HashMap<PageId, Page> buffer;
    private int numPages;
    int currentId;
    long fileSize;
    
   /* public boolean hasNext() {
    	RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "r");
	    	boolean ans=currentId*getPageSize() < raf.length();
	    	raf.close();
	    	return ans;
		} catch (IOException e) {
			return false;
		}
    }
    public void next() throws IOException{
    	buffer.clear();
    	byte[] data=new byte[getPageSize()];
    	RandomAccessFile raf=new RandomAccessFile(f, "r");
    	raf.seek(currentId*getPageSize());
    	for(int i=0;i<numPages;i++) {
    		int id=currentId;
    		currentId++;
    		try {
    			raf.read(data);
    		}catch (Exception e) {
    			break;
			}
    		buffer.add(new HeapPage(new HeapPageId(tableid,id), data));
    	}
    	raf.close();
    }*/
    public BufferPool(int numPages) {
        buffer=new HashMap<PageId, Page>();
        this.numPages=numPages;
        this.currentId=0;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    private void addPage(Page page) {
    	if(buffer.containsKey(page.getId())) {
    		buffer.remove(page.getId());
    		buffer.put(page.getId(),page);
    	}else {
        	if(buffer.size()>=numPages) {
        		try {
    				evictPage();
    			} catch (DbException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
        	}
        	buffer.put(page.getId(),page);
    	}
    	
	}
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if(buffer.containsKey(pid))
        	return buffer.get(pid);
    	  Page page=Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    	  if(perm==Permissions.READ_WRITE)
    		  page.markDirty(true, tid);
    	  addPage(page);
      
        
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        throw new NotImplementedException();

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        throw new NotImplementedException();

    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        throw new NotImplementedException();

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> arrayList=Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	for(Page page:arrayList) {
    		page.markDirty(true, tid);
    		addPage(page);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	ArrayList<Page> arrayList=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    	for(Page page:arrayList) {
    		page.markDirty(true, tid);
    		addPage(page);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	while(buffer.size()>0) {
    		try {
				evictPage();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	try {
			flushPage(buffer.values().iterator().next().getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	if(buffer.get(pid).isDirty()!=null)
    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(buffer.get(pid));

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        throw new NotImplementedException();

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        if(buffer.isEmpty())
        	throw new DbException("evict");
      
      discardPage(buffer.values().iterator().next().getId());
    }

}
