package simpledb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.tools.DocumentationTool.Location;

import com.sun.media.sound.RIFFInvalidDataException;
import com.sun.swing.internal.plaf.synth.resources.synth;

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
    LinkedHashMap<PageId, Page> buffer;
    private int numPages;
    int currentId;
    long fileSize;
    class Triple{
    	public TransactionId tid;
    	public PageId pid;
    	public Permissions perm;
    	public Triple(TransactionId tid,PageId pid,Permissions perm) {
    		this.tid=tid;
    		this.pid=pid;
    		this.perm=perm;
		}
        public int hashCode() {
        	return (tid.hashCode()<<16)^pid.hashCode();
        }
        public boolean equals(Object o)
        {
        	if(o==null)return false;
        	if(o instanceof Triple) {
        		return tid.equals(((Triple) o).tid) 
            			&& pid.equals(((Triple) o).pid) 
            			&& perm.equals(((Triple) o).perm);
        	}
        	return false;
        }
    }
    enum PageStatus{
    	NONE,ONE_READ,ONE_WRITE,MULTI_READ
    }
    class LockManager{
	    private ConcurrentHashMap<TransactionId, ArrayList<Triple> > usedPageMap;
	    private ConcurrentHashMap<PageId, ArrayList<Triple>> belongMap;
    	public LockManager() {
			usedPageMap=new ConcurrentHashMap<TransactionId, ArrayList<Triple>>();
			belongMap=new ConcurrentHashMap<PageId, ArrayList<Triple>>();
		}
    	ArrayList<Triple> getByPid(PageId pid){
    		ArrayList<Triple>arrayList=belongMap.get(pid);
    		if(arrayList==null) 
    			belongMap.put(pid,new ArrayList<Triple>());
    		return belongMap.get(pid);
    	}
    	ArrayList<Triple> getByTid(TransactionId tid){
    		ArrayList<Triple>arrayList=usedPageMap.get(tid);
    		if(arrayList==null) 
    			usedPageMap.put(tid,new ArrayList<Triple>());
    		return usedPageMap.get(tid);
    	}
	    Triple getTriple(TransactionId tid,PageId pid) {
	    	ArrayList<Triple>arrayList=getByPid(pid);
	    	for(Triple triple:arrayList) {
	    		if(triple.tid.equals(tid))
	    			return triple;
	    	}
	    	return null;
	    }
	    void removeTriple(Triple triple) {
	    	getByPid(triple.pid).remove(triple);
	    	getByTid(triple.tid).remove(triple);
	    }
	    void updateTriple(Triple triple) {
	    	removeTriple(new Triple(triple.tid, triple.pid, Permissions.READ_ONLY));
	    	removeTriple(new Triple(triple.tid, triple.pid, Permissions.READ_WRITE));
	    	getByPid(triple.pid).add(triple);
	    	getByTid(triple.tid).add(triple);
	    }
	    PageStatus getPageStatus(PageId pid) {
	    	ArrayList<Triple> arrayList=getByPid(pid);
	    	if(arrayList.size()==1) {
	    		if(arrayList.get(0).perm.equals(Permissions.READ_ONLY))
	    			return PageStatus.ONE_READ;
	    		return PageStatus.ONE_WRITE;
	    	}
	    	if(arrayList.size()>1) {
	    		return PageStatus.MULTI_READ;
	    	}
	    	return PageStatus.NONE;
	    }
	    public synchronized boolean getLock(TransactionId tid,PageId pid,Permissions perm) {
	    	//assume all READ_WRITE
	    	
	    	PageStatus pageStatus=getPageStatus(pid);
	    	ArrayList<Triple> arrayList=getByPid(pid);
	    	Triple triple=new Triple(tid, pid, perm);
	    	if(pageStatus==PageStatus.NONE) {
	    		updateTriple(triple);
	    		return true;
	    	}
	    	if(pageStatus==PageStatus.ONE_READ) {
	    		if(arrayList.get(0).tid.equals(tid)) {
	    			updateTriple(triple);
	    			return true;
	    		}else {
	    			if(perm.equals(Permissions.READ_WRITE))
	    				return false;
	    			updateTriple(triple);
	    			return true;
	    		}
	    	}
	    	if(pageStatus==PageStatus.ONE_WRITE) {
	    		return arrayList.get(0).tid.equals(tid); 	
	    	}
	    	if(pageStatus==PageStatus.MULTI_READ) {
	    		if(perm.equals(Permissions.READ_ONLY)) {
	    			updateTriple(triple);
	    			return true;
	    		}else {
	    			return false;
	    		}
	    	}
	    	
	    	return false;
	    }
    }
    LockManager lockManager;
    public BufferPool(int numPages) {
        buffer=new LinkedHashMap<PageId, Page>();
        this.numPages=numPages;
        this.currentId=0;
        this.lockManager=new LockManager();
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
    public  synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	while(!lockManager.getLock(tid, pid, perm)) {
    		//transactionComplete(tid,false);
    		try {
				wait(10);
			} catch (InterruptedException e) {
			}
    	}
    	
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
    	lockManager.removeTriple(new Triple(tid, pid, Permissions.READ_WRITE));

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

    	return lockManager.getTriple(tid, p)!=null;
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

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        if(buffer.isEmpty())
        	throw new DbException("evict");
      Iterator<Page> iterator= buffer.values().iterator();
      while(iterator.hasNext()) {
    	  Page page=iterator.next();
    	  if(page.isDirty()==null) {
    		  discardPage(page.getId());
    	  }
      }
  		throw new DbException("evict");
      
    }

}