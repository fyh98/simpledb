package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.print.attribute.SetOfIntegerSyntax;











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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private final ConcurrentHashMap<Integer,Page> pages;
    ConcurrentHashMap<TransactionId,Vector<PageId>> bucket;
    private  LockManager lockManager;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages=numPages;
    	pages=new ConcurrentHashMap<Integer,Page>();
        lockManager = new LockManager();
        bucket=new ConcurrentHashMap<TransactionId, Vector<PageId>>();
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     * @throws DeadlockException 
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
	    
    	int lockType;
    	boolean isLockShare;
        if(perm == Permissions.READ_ONLY) {
            lockType = 0;
            isLockShare=true;
        }else{
            lockType = 1;
            isLockShare=false;
        }
        
        Integer keyPid=pid.hashCode();
        Page pageResult=getPageFromBuffer(pid);
        if(!lockManager.isSameLock(pageResult, tid, isLockShare))
            lockManager.acquireLock(pageResult, tid, isLockShare);
    	return  pageResult;
    }
	
    private Page getPageFromBuffer(PageId pid) throws DbException {
    	Integer keyPid=pid.hashCode();
        Page pageResult;
        synchronized (pages) {
            if(!pages.containsKey(keyPid)) {
                int tabId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tabId);
                Page page = file.readPage(pid);     
                    if(pages.size()==numPages) {
                        evictPage();
                    }
                    pages.put(keyPid,page);
            }
            pageResult=pages.get(keyPid);
        }
        return pageResult;
    }
    
    void getPageSet(Page page) {
    	PageId pid=page.getId();
    	Integer keyPid=page.getId().hashCode();
        Page pageResult;
        synchronized (pages) {
             
                pages.put(keyPid,page);
            
    
		}
        
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
    	Page pageResult = null;
	try {
	    pageResult = getPageFromBuffer(pid);
	} catch (DbException e) {
	    e.printStackTrace();
	}
    	lockManager.releaseLock(pageResult,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	Page pageResult = null;
	try {
	    pageResult = getPageFromBuffer(tid, p);
	} catch (DbException e) {
	    e.printStackTrace();
	}
	return lockManager.holdsLock(pageResult,tid);
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
    	
    	if(commit){
            flushPagesForCommit(tid);
        }else{
            restorePages(tid);
        }
    	bucket.remove(tid);	
    	int count=0;
    	Vector<PageId> pageIds=lockManager.reverseLockMap.get(tid);
    	if(pageIds!=null) {
    	    while (pageIds.size()!=0) {
        	releasePage(tid, pageIds.get(0));			
    	    }
    	}
    }
    
   /** Restore pages that Transaction tid has updated */
    private void restorePages(TransactionId tid) {
    	synchronized (pages) {
    	    for (Integer pid : pages.keySet()) {
                Page page = pages.get(pid);
                if (page.isDirty()!=null&&page.isDirty().equals(tid)) {
                    int tabId = page.getId().getTableId();
                    DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                    Page pageBeforeImage =page.getBeforeImage();
                    try {
    		        file.writePage(pageBeforeImage);
    		    } catch (IOException e) {
        	        e.printStackTrace();
    		    }
                    pages.put(pid, pageBeforeImage);
               }
            }
	}
     }

     /** check deadlock and throws a TransactionAbortedException if there is  deadlock */	
     void  checkDeadLock() throws TransactionAbortedException {
         node cur=null;
    	 node pre=null;
    	 ConcurrentHashMap<TransactionId,node> map = new ConcurrentHashMap<TransactionId, node>();
    	 ConcurrentHashMap<TransactionId,Vector<PageId>> copyOfBucket=new ConcurrentHashMap<TransactionId,Vector<PageId>>();
    	 ConcurrentHashMap<PageId,Vector<Lock>> copyOfLockmap=new ConcurrentHashMap<PageId, Vector<Lock>>();
    	 boolean isDeadLock=false;	
    	 synchronized (bucket) {
    	     copyOfLockmap.putAll(lockManager.lockMap);
    	     for(Map.Entry<TransactionId, Vector<PageId>> entry:bucket.entrySet()) {
        	 Vector<PageId> pageNeed=entry.getValue();
        	 TransactionId tId=entry.getKey();        			
        	 if(map.get(tId)==null) {
    		     map.put(tId, new node(tId));
    		 }	 
                 cur=map.get(tId);
                 for(int i=0;i<pageNeed.size();++i) {
                     PageId pid=pageNeed.get(i);
        	     Vector<Lock> locksUsing;   					
        	     locksUsing=copyOfLockmap.get(pid);
        	     for(int j=0;j<locksUsing.size();++j) {
        	         TransactionId tidUsing=locksUsing.get(j).tid;
        	         if(tId.equals(tidUsing))
        	             continue;			
        	         if(map.get(tidUsing)==null) {
        	             map.put(tidUsing, new node(tidUsing));
        	         }
        	         node nodeRely=map.get(tidUsing);
        	         cur.require.add(nodeRely);		
        	    }		
                 }		
             }    			
	 }
    	 node[] nodeRemove=new node[1];	
    	 for(Map.Entry<TransactionId, node> entry:map.entrySet()) {
    	     node nodeCur=entry.getValue();
    	     if(nodeCur.visit==1)
    	         continue;
    	     if(!dfs(nodeCur)) {
		 throw new TransactionAbortedException();
    	     }
    	 }	
    }

    /** implementation of  Topology traversal algorithm */	
    boolean dfs(node nodeNow) {
        if(nodeNow.visit==-1)
    	    return false;
    	nodeNow.visit=-1;
    	for(int i=0;i<nodeNow.require.size();++i) {
    	    if(nodeNow.require.get(i).visit==-1)
    	        return false;
    	    if(nodeNow.require.get(i).visit==1)
    		continue;    		
    	    if(!dfs(nodeNow.require.get(i))) {
    		return false;
    	    }
    	}
    	nodeNow.visit=1;
    	return true;
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
     * @throws DeadlockException 
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile file=Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pageList=file.insertTuple(tid, t);
    	for(Page page:pageList) {
    	    page.markDirty(true, tid);
    	    if(pages.size()>numPages)
    	    evictPage();
    	    pages.put(page.getId().hashCode(), page);
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
     * @throws DeadlockException 
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile file=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> pageList=file.deleteTuple(tid, t);
    	for(Page page:pageList) {
    	    page.markDirty(true, tid);
    	    if(pages.size()>numPages)
    	        evictPage();
    		pages.put(page.getId().hashCode(), page);
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
    	for(Page p:pages.values()) {
    	    flushPage(p.getId());	
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public  void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	synchronized (pages) {
    	    pages.remove(pid.hashCode());
	}    	
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1    	
    	Page page=getPageFromBuffer(pid);
    	TransactionId tId=page.isDirty();    	    	
    	if(tId!=null) {
    	    Database.getLogFile().logWrite(tId,page.getBeforeImage(),page);
    	    Database.getLogFile().force();
    	    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    	    page.markDirty(false, null);
    	}    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Iterator<Map.Entry<PageId, Vector<Lock>>> iterator=lockManager.lockMap.entrySet().iterator();
    	while (iterator.hasNext()) {
	    Map.Entry<PageId, Vector<Lock>> now=iterator.next();
	    PageId pageNowId=now.getKey();
	    Vector<Lock> locksNow=now.getValue();
	        for(int i=0;i<locksNow.size();++i) {
		    if(locksNow.get(i).tid.equals(tid))
		        flushPage(pageNowId);
		}
	}    	
    }
    public  void flushPagesForCommit(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2    	
    	Vector<PageId> pageIds=lockManager.reverseLockMap.get(tid);
    	if(pageIds==null)
    		return;
    	for(int i=0;i<pageIds.size();++i) {
    		 flushPage(pageIds.get(i));
    		 Page page=getPageSimple(pageIds.get(i));
			page.setBeforeImage();
    	}
    }
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	boolean leap=false;
    	synchronized (pages) {
    	    List<Page> pageList=new ArrayList<Page>(pages.values());
            for(int i=0;i<pageList.size();++i) {
                Page page=pageList.get(i);
                if(page.isDirty()!=null) {
                    continue;
                }
                discardPage(page.getId());
                leap=true;
                break;
            }
	}
    	if(!leap)
    	    throw new DbException("all pages are dirty");
    }
	
    private class node{
    	TransactionId tId;
    	Vector<node> require;
    	int visit;
    	public node(TransactionId tId) {
    	    this.tId=tId;
    	    require=new Vector<>();
    	    visit=0;
	}    	
    }
    
    private class Lock{
        TransactionId tid;
           // 0 for shared lock and 1 for exclusive lock
        boolean isLockShare;
        public Lock(TransactionId tid,boolean isLockShare){
            this.tid = tid;
            this.isLockShare=isLockShare;
        }
    }
	
    private class LockManager{
        ConcurrentHashMap<PageId,Vector<Lock>> lockMap;
        ConcurrentHashMap<PageId,TransactionId> specialRelease;
        ConcurrentHashMap<PageId, Vector<TransactionId>> specialRelease1;
       ConcurrentHashMap<TransactionId, Vector<PageId>> reverseLockMap;
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, PageId>> pageIdsOfBTreePage;
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, PageId>> pageIdsOfHeapPage;
        Vector<TransactionId> transactionIds1;
        
        public LockManager(){
            lockMap = new ConcurrentHashMap<PageId,Vector<Lock>>();
            specialRelease=new ConcurrentHashMap<PageId, TransactionId>();
            specialRelease1=new ConcurrentHashMap<PageId, Vector<TransactionId>>();
            transactionIds1=new Vector<>();
            pageIdsOfBTreePage=new ConcurrentHashMap<>();
            pageIdsOfHeapPage=new ConcurrentHashMap<>();
            reverseLockMap=new ConcurrentHashMap<TransactionId, Vector<PageId>>();
        }
	    
        private Boolean isBTreePageId(PageId pageId) {
            HeapPageId pageIdToBeCompared=new HeapPageId(pageId.getTableId(), pageId.getPageNumber());
            if(pageIdToBeCompared.equals(pageId))
                return false;
            else
        	return true;
	}
	    
        private PageId getPageId(PageId pageId) {
        	
        	HeapPageId pageIdToBeCompared=new HeapPageId(pageId.getTableId(), pageId.getPageNumber());
        	Boolean isBTreePageId=isBTreePageId(pageId);
        	if(isBTreePageId) {
        		if(pageIdsOfBTreePage.get(pageId.getTableId())==null) {
        			pageIdsOfBTreePage.put(pageId.getTableId(), new ConcurrentHashMap<Integer, PageId>());
        		}
        		if(pageIdsOfBTreePage.get(pageId.getTableId()).get(pageId.getPageNumber())==null) {
        			ConcurrentHashMap<Integer, PageId> curPageIds=pageIdsOfBTreePage.get(pageId.getTableId());
        			curPageIds.put(pageId.getPageNumber(), pageId);
        		}
        		return 	pageIdsOfBTreePage.get(pageId.getTableId()).get(pageId.getPageNumber());
        	}
        	else {
        		if(pageIdsOfHeapPage.get(pageId.getTableId())==null) {
        			pageIdsOfHeapPage.put(pageId.getTableId(), new ConcurrentHashMap<Integer, PageId>());
        		}
        		if(pageIdsOfHeapPage.get(pageId.getTableId()).get(pageId.getPageNumber())==null) {
        			ConcurrentHashMap<Integer, PageId> curPageIds=pageIdsOfHeapPage.get(pageId.getTableId());
        			curPageIds.put(pageId.getPageNumber(), pageId);
        		}
        		return 	pageIdsOfHeapPage.get(pageId.getTableId()).get(pageId.getPageNumber());
			}
        }
        public  void acquireLock(Page page,TransactionId tid,boolean isLockShare) throws  TransactionAbortedException{
            // if no lock held on pid
        	PageId pid=page.getId();
        	Lock lock=new Lock(tid, isLockShare);
        	boolean isWait=false;
        	boolean start=false;
        	
        	PageId realPageId=getPageId(pid);
        	pid=realPageId;
        	if(reverseLockMap.get(tid)==null)
        		reverseLockMap.put(tid, new Vector<>());
        	
   //    	System.out.println(tid+" "+ isLockShare+"  acquire"+pid.getPageNumber());
        	synchronized (realPageId) {
        		
				if(lockMap.get(pid)==null) {
	//				if(leap)
	//				System.out.println(tid+"come1");
					Vector<Lock> locks=new Vector<>();
					locks.add(lock);
					lockMap.put(pid, locks);
					reverseLockMap.get(tid).add(pid);
					
		//			System.out.println(tid+" "+reverseLockMap.get(tid).size());
				}
				else {
					Vector<Lock> locks=lockMap.get(pid);
					
					if(isLockShare) {
		//				if(leap)
		//					System.out.println(tid+"come2");
						boolean condition1=(locks.size()==0)||((locks.size()!=0)&&(locks.get(0).isLockShare));
						boolean condition2=locks.size()>0&&!locks.get(0).isLockShare&&locks.get(0).tid.equals(tid);
						
		//				if(leap)
		//					System.out.println(tid+"calculated2");
						while (!(condition1||condition2)) {
							isWait=true;
							
							try {
		//						if(leap)
		//							System.out.println(tid+" come2.1");
								
		//							if(leap)
		//								System.out.println(tid+"come2.2");
								synchronized (bucket) {
									if(!start) {
										start=true;
										Vector<PageId> pageNeed;
										if(bucket.get(tid)!=null)
											pageNeed=bucket.get(tid);
										else {
											pageNeed=new Vector<>();
											bucket.put(tid, pageNeed);
										}					
										pageNeed.add(pid);
										
									}
								}
									
				//				System.out.println("deadlock check "+tid+" "+pid.getPageNumber());
									
				//					System.out.println("deadlock check end"+tid);
							
								
								
								checkDeadLock();
								
		
	//							System.out.println(tid+" wait for"+pid.getPageNumber()+" "+isLockShare);
								pid.wait();
								locks=lockMap.get(pid);
	//							System.out.println(tid+" gets up "+pid.getPageNumber()+" "+isLockShare);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							 condition1=(locks.size()==0)||((locks.size()!=0)&&(locks.get(0).isLockShare));
							 condition2=locks.size()>0&&!locks.get(0).isLockShare&&locks.get(0).tid.equals(tid);
						}
	
	//					if(leap)
	//						System.out.println(tid+"stop waiting");
						
							synchronized (bucket) {
								if(isWait) {
									Vector<PageId> pageNeed=bucket.get(tid);
									for(int i=0;i<pageNeed.size();++i) {
										if(pageNeed.get(i).equals(pid)) {
											pageNeed.remove(i);
									
											break;
										}
									}
									bucket.put(tid, pageNeed);
								}
							}
							
						
						
						
							
						Vector<Lock> copyOfLocks=new Vector<>(locks.size());
						for(int i=0;i<copyOfLocks.size();++i) {
							Lock curLock=locks.get(i);
							copyOfLocks.set(i, new Lock(curLock.tid, curLock.isLockShare));
						}
						copyOfLocks.add(lock);	
		//				locks.add(lock);
		//				System.out.println(tid+" add"+pid.getPageNumber()+" "+locks.size());
		//				lockMap.put(pid, locks);
						lockMap.put(pid, copyOfLocks);
						reverseLockMap.get(tid).add(pid);
						
		//				System.out.println(tid+"wait"+pid.getPageNumber()+" S "+condition1+" "+condition2);
					}
					else {
	//					if(leap)
	//						System.out.println(tid+"come3");
						boolean condition1=locks.size()==0;
						boolean condition2=(locks.size()==1)&&locks.get(0).isLockShare&&locks.get(0).tid.equals(tid);
	//					if(leap)
	//						System.out.println(tid+"calculated3");
						while(!(condition1||condition2)) {
			//				System.out.println(locks.size()+" "+locks.get(0).isLockShare+" "+locks.get(0).tid+" "+tid);
						
			
					//		System.out.println(tid+"wait"+pid.getPageNumber()+" X "+condition1+" "+condition2);
							isWait=true;
							if(!condition1) {
								boolean hasSame=false;
								boolean meetNeed=true;
								for(int i=0;i<locks.size();++i) {
									if(!locks.get(i).isLockShare) {
										meetNeed=false;
										break;
									}
									if(locks.get(i).tid.equals(tid))
										hasSame=true;
								}
								if(meetNeed&&hasSame) {
									specialRelease.put(pid, tid);
									if(specialRelease1.get(pid)==null) {
										Vector<TransactionId> transactionIds=new Vector<>();
										specialRelease1.put(pid, transactionIds);
									}
									Vector<TransactionId> transactionIds=specialRelease1.get(pid);
									transactionIds.add(tid);
								}
									
							}
							try {
		//						if(leap)
		//							System.out.println(tid+"come3.1");
								
		//							if(leap)
		//								System.out.println(tid+"come3.2");
								synchronized (bucket) {
									if(!start) {
										start=true;
										Vector<PageId> pageNeed;
										if(bucket.get(tid)!=null)
											pageNeed=bucket.get(tid);
										else {
											pageNeed=new Vector<>();
											bucket.put(tid, pageNeed);
										}
								
										pageNeed.add(pid);
										
									}
								}
									
									
									//bucket.put(tid, pageNeed);
		//							System.out.println("deadlock check"+tid+" "+pid.getPageNumber());
									
		//							System.out.println("deadlock check end"+tid);
								
							
								checkDeadLock();
								
							//	if(page.getId().getPageNumber()==52) {
							//		transactionIds1.add(tid);
							//		System.out.println(page.toString()+" "+page.getId());
							//	}
		//						System.out.println(tid+" wait for "+pid.getPageNumber()+" "+isLockShare);
								pid.wait();
								locks=lockMap.get(pid);
									
		//						System.out.println(tid+" gets up "+pid.getPageNumber()+" "+isLockShare);
						
								
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							condition1=locks.size()==0;
							condition2=(locks.size()==1)&&locks.get(0).isLockShare&&locks.get(0).tid.equals(tid);
						}
			//			if(leap)
			//				System.out.println(tid+"stop waiting");
					
						synchronized (bucket) {
							if(isWait) {
								Vector<PageId> pageNeed=bucket.get(tid);
								for(int i=0;i<pageNeed.size();++i) {
									if(pageNeed.get(i).equals(pid)) {
										pageNeed.remove(i);
								
										break;
									}
								}
								bucket.put(tid, pageNeed);
							}
						}	
							
						Vector<Lock> copyOfLocks=new Vector<>(locks.size());
						for(int i=0;i<copyOfLocks.size();++i) {
							Lock curLock=locks.get(i);
							copyOfLocks.set(i, new Lock(curLock.tid, curLock.isLockShare));
						}
							
						
						
						
						if(condition1) {
							//locks.add(lock);
							copyOfLocks.add(lock);
							//lockMap.put(pid, locks);
							lockMap.put(pid, copyOfLocks);
						}
						else {
							//locks.clear();
							//locks.add(lock);
							//lockMap.put(pid, locks);
							copyOfLocks.clear();
							copyOfLocks.add(lock);
							lockMap.put(pid, copyOfLocks);
						}
	//					System.out.println(tid+" add"+pid.getPageNumber()+"  "+locks.size());
						reverseLockMap.get(tid).add(pid);
						
					}
					
				}
			}
   	
   //     	 	System.out.println(tid+" get "+pid.getPageNumber()+" "+isLockShare);
        }
 
 
        public boolean releaseLock(Page page,TransactionId tid){
            // if not a single lock is held on pid
        	PageId pid=page.getId();
  //      	System.out.println("release "+page.getId()+" "+tid);
        	/*synchronized (page) {
				if(lockMap.get(pid) == null)
					return false;
				Vector<Lock> locks=lockMap.get(pid);
				int pointer=-1;
				for(int i=0;i<locks.size();++i) {
					if(locks.get(i).tid.equals(tid)) {
						pointer=i;
						break;
					}
				}
				if(pointer<0)
					return false;
				
				if(locks.get(pointer).isLockShare) {
					locks.remove(pointer);
					if(locks.size()==0)
						page.notify();
					return true;
				}
				else {
					locks.remove(pointer);
					page.notify();
					return true;
				}
			}*/
        	pid=getPageId(page.getId());
        	//System.out.println(tid+"will enter "+pid.getPageNumber()+" releaselock");
        	synchronized (pid) {
        		
				if(lockMap.get(pid) != null) {
					Vector<Lock> locks=lockMap.get(pid);
					Vector<Integer> indexs=new Vector<>();
					for(int i=0;i<locks.size();++i) {
						if(locks.get(i).tid.equals(tid)) {
							indexs.add(i);
						}
					}
					int len=indexs.size();
					if(indexs.size()!=0) {
						Vector<Lock> copyOfLocks=new Vector<>();
						for(int i=0;i<locks.size();++i) {
							Lock curLock=locks.get(i);
							copyOfLocks.add(new Lock(curLock.tid, curLock.isLockShare));
						}
						for(int i=0;i<indexs.size();++i) {
							
							//locks.remove(indexs.get(i)-i);
							copyOfLocks.remove(indexs.get(i)-i);
						}
						lockManager.lockMap.put(pid, copyOfLocks);
						//if(locks.size()!=0) {
						if(copyOfLocks.size()!=0) {
							//if(locks.size()==1&&specialRelease.get(pid).equals(locks.get(0).tid))
							//	page.notify();
						//	if(locks.size()==1) {
							if(copyOfLocks.size()==1) {
								Vector<TransactionId> transactionIds=specialRelease1.get(pid);
								
								if(transactionIds!=null&&copyOfLocks.get(0).isLockShare) {
									for(int i=0;i<transactionIds.size();++i) {
										
										if(transactionIds.get(i).equals(copyOfLocks.get(0).tid)) {
											pid.notify();
											break;
										}
											
									}
								}
									
								
							}
						}
						else {
				
							pid.notify();
						}
					}
				}
				/*Vector<Lock> locks=lockMap.get(pid);
				
				Vector<Integer> indexs=new Vector<>();
				for(int i=0;i<locks.size();++i) {
					if(locks.get(i).tid.equals(tid)) {
						indexs.add(i);
					}
				}
				int len=indexs.size();
				if(indexs.size()==0)
					return false;
				
			//	System.out.println("remove size:"+indexs.size());
				for(int i=0;i<indexs.size();++i) {
					locks.remove(indexs.get(i)-i);
				}
			//	System.out.println("remove"+len+" still "+locks.size()+" "+pid.getPageNumber());
				if(locks.size()!=0) {
					//if(locks.size()==1&&specialRelease.get(pid).equals(locks.get(0).tid))
					//	page.notify();
					if(locks.size()==1) {
						Vector<TransactionId> transactionIds=specialRelease1.get(pid);
						
						if(transactionIds!=null&&locks.get(0).isLockShare) {
							for(int i=0;i<transactionIds.size();++i) {
								
								if(transactionIds.get(i).equals(locks.get(0).tid)) {
									pid.notify();
									break;
								}
									
							}
						}
							
						
					}
				}
				else {
		
					pid.notify();
				}
				
				//System.out.println(tid+" release "+ page.getId().getPageNumber()+" "+locks.size());
				
				*/
			}
        	
        	Vector<PageId> pIds=reverseLockMap.get(tid);
        	
        	while (pIds.remove(pid)) {	
        		
        	}
        	
        	
        	return true;
        }
 
        public boolean isSameLock(Page page,TransactionId tid,boolean isLockShare) {
        	PageId pid=page.getId();
        	pid=getPageId(page.getId());
        	boolean result=false;
        	//System.out.println(tid+" get into isSamelock");
       // 	System.out.println(tid+"will enter "+pid.getPageNumber()+" issamelock");
        	synchronized (pid) {
       // 		System.out.println(tid+"has enter "+pid.getPageNumber()+" issamelock");
        		if(lockMap.get(pid) != null) {
        			Vector<Lock> locks = lockMap.get(pid);
            		for(Lock lock:locks){
                        if((lock.tid == tid)&&((lock.isLockShare^isLockShare)^true)){
                           result=true;
                        	break;
                        }
                    }
        		}
			}
      //  	System.out.println(tid+"will quit "+pid.getPageNumber()+" issamelock");
        	return result;
        }
        public  boolean holdsLock(Page page,TransactionId tid){
            // if not a single lock is held on pid
        	PageId pid=page.getId();
        	pid=getPageId(page.getId());
        	boolean result=false;
        	synchronized (pid) {
        		if(lockMap.get(pid) == null)
                    result= false;
        		else {
        			Vector<Lock> locks = lockMap.get(pid);
            		for(Lock lock:locks){
                        if(lock.tid == tid){
                            result= true;
                        }
                    }
				}
        		
               
			}  
        	return result;
        }
    }
}
