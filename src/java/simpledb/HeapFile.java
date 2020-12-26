package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private  File f;
	private  TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f=f;
    	this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int tableId=pid.getTableId();
    	int pgno=pid.getPageNumber();
    	//System.out.println("pgno:"+pgno);
    	RandomAccessFile file=null;
    	
    	try {
    		file=new RandomAccessFile(f, "r");
    		if((pgno+1)*BufferPool.getPageSize()>file.length()) {
    			file.close();
    			throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgno));
    		}
    		
    		byte[] bytes=new byte[BufferPool.getPageSize()];
    		file.seek(pgno*BufferPool.getPageSize());
    		
    		int read=file.read(bytes,0,BufferPool.getPageSize());
    	
    		if(read != BufferPool.getPageSize()){                
    			throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgno, read));            
    		}
    		HeapPageId heapPageId=new HeapPageId(pid.getTableId(),pid.getPageNumber());
    		return new HeapPage(heapPageId,bytes);
    	}
    	catch (Exception e) {
			// TODO: handle exception
		}
		return null;
    	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	RandomAccessFile file=null;
    	file=new RandomAccessFile(f, "rw");
    	byte[] bytes=page.getPageData();
    	int pgno=page.getId().getPageNumber();
    	file.seek(pgno*BufferPool.getPageSize());
    	file.write(bytes);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int num = (int)Math.ceil((f.length()*1.0)/BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	/*ArrayList<Page> pages=new ArrayList<>();
    	for(int i=0;i<numPages();++i) {
    		HeapPageId pageId=new HeapPageId(this.getId(), i);
    		HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid,pageId, Permissions.READ_WRITE);
    		if(page.getNumEmptySlots()==0)
    			continue;
    		else {
    			page.insertTuple(t);
    			pages.add(page);
    			return pages;
    		}
    	}
    	BufferedOutputStream bufferedOutputStream=new BufferedOutputStream(new FileOutputStream(f,true));
    	byte[] emptyData=HeapPage.createEmptyPageData();
    	bufferedOutputStream.write(emptyData);
    	bufferedOutputStream.close();
    	HeapPageId newPid=new HeapPageId(this.getId(), numPages()-1);
    	HeapPage newPage=(HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
    	newPage.insertTuple(t);
    	pages.add(newPage);
        return pages;*/
    	HeapPage page  = null;
    	boolean find=false;
    	HeapPageId pid = null;
        // find a non full page
        for(int i=0;i<numPages();++i){
        	
             pid = new HeapPageId(getId(),i);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            
            if(page.getNumEmptySlots()!=0){
            	find=true;
                break;
            }
            else{
            	
                Database.getBufferPool().releasePage(tid,pid);
            }
        }
        
        // if not exist an empty slot, create a new page to store
        //if(page == null || page.getNumEmptySlots() == 0){
        if(!find) {
             pid = new HeapPageId(getId(),numPages());
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(pid,data);
           // System.out.println(2);
            synchronized (f) {
            	writePage(heapPage);
			}
            
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        }
       
        page.insertTuple(t);
        
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
      //  Database.getBufferPool().releasePage(tid,pid);
        return res;

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException{
        // some code goes here
    	/*ArrayList<Page> pages=new ArrayList<>();
    	HeapPage page=(HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	pages.add(page);
        return pages;*/
    	RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
 
        // delete tuple and mark page as dirty
        HeapPage page =  (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        page.deleteTuple(t);
 
        // return res
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
     //   Database.getBufferPool().releasePage(tid,pid);
        return res;

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(this,tid);
    }
    private static final class HeapFileIterator implements DbFileIterator{
    	private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int whichPage;
        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        public void open() throws DbException, TransactionAbortedException{           
        	whichPage = 0;            
        	
        	it = getPageTuples(whichPage);        
        }
        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException,DbException{
        	
        	if(pageNumber>=0&&pageNumber<heapFile.numPages()) {
        		
        		HeapPageId pid=new HeapPageId(heapFile.getId(), pageNumber);
        //		System.out.println("pid:"+pid);
        		HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        		//HeapPage page = (HeapPage)Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        		//System.out.println("numSlots:"+page.numSlots);
        		//System.out.println(page.header[0]);
                return page.iterator();
        	}
        	else {
        //		System.out.println("*******************************");
        		throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
			}
        }
        public boolean hasNext() throws DbException, TransactionAbortedException{
        	if(it==null) {
        		return false;
        	}
        	if(!it.hasNext()) {
        		if(whichPage<(heapFile.numPages()-1)) {
        			++whichPage;
        			it=getPageTuples(whichPage);
        			return it.hasNext();
        		}
        		else {
					return false;
				}
        	}
        	else {
        		return true;
        	}
        }
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {            
        	// TODO Auto-generated method stub            
        	if(it == null || !it.hasNext()){                
        		throw new NoSuchElementException();            
        	}            
        	return it.next();        
        }
        public void rewind() throws DbException, TransactionAbortedException{
        	
        	close();
        	open();
        }
        public void close() {
        	it=null;
        }
    }
}

