
package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;



import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
            	//System.out.println("before abort:"+raf.getFilePointer()+" "+tid.myid);
            	//System.out.println(recoveryUndecided);
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
               
                rollback(tid);
              //  System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
              //  System.out.println("write abort");
              //  System.out.println("pointer1:"+raf.getFilePointer());
                raf.writeInt(ABORT_RECORD);
                
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                
                force();
                tidToFirstLogRecord.remove(tid.getId());
             //   System.out.println("pointer2:"+raf.getFilePointer());
             //   System.out.println(raf.length());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        
        //should we verify that this is a live transaction?
        
      //  System.out.println("write commit"+" "+tid.getId());
      //  System.out.println("pointer1:"+raf.getFilePointer());
        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        
        force();
        tidToFirstLogRecord.remove(tid.getId());
    //    System.out.println("pointer2:"+raf.getFilePointer());
    //    System.out.println(raf.length());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see simpledb.Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
   //     System.out.println("write update"+" "+tid.getId());
    //    System.out.println("pointer1:"+raf.getFilePointer());
        raf.writeInt(UPDATE_RECORD);
        
        raf.writeLong(tid.getId());
        
        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
   //     System.out.println("pointer2:"+raf.getFilePointer());
  //      System.out.println(raf.length());
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
       
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
     //   System.out.println("write begin"+" "+tid.myid);
     //   System.out.println("pointer1:"+raf.getFilePointer());
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();
    //    System.out.println("pointer2:"+raf.getFilePointer());
    //    System.out.println(raf.length());
        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
    	
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience
        //        System.out.println("write checkpoint");
                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }
    //    System.out.println("truncate**********************");
        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();
     //           System.out.println(type+"********************************************");
                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();
      //          System.out.println(newStart);
            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                
                Long checkPointOffset,lastRecordOffset;
                //lastRecordOffset=tidToFirstLogRecord.get(tid.getId());
            //    System.out.println("tid: "+tid.getId());
                raf.seek(raf.length()-8);
                lastRecordOffset=raf.readLong();
                long beginPointOffset=tidToFirstLogRecord.get(tid.getId());
               // raf.seek(0);
               // checkPointOffset=this.raf.readLong();
                
                while (lastRecordOffset>=beginPointOffset) {
                	raf.seek(lastRecordOffset);
                	int typeRecord=raf.readInt();
                	long transactionIDNumber=raf.readLong();
                	//System.out.println(typeRecord+" "+transactionIDNumber);
                	if((typeRecord==UPDATE_RECORD)&&(transactionIDNumber==tid.getId())) {
                		HeapPage oldPage=(HeapPage) readPageData(raf);
                		//oldPage.markDirty(true, tid);
                		HeapPage newPage=(HeapPage)readPageData(raf);
                		HeapPageId pageId=oldPage.getId();
                		HeapPage heapPage1=(HeapPage) Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
                	//	System.out.println("found 4 before writng0?"+found(4, heapPage1));
                	//	System.out.println(pageId.getPageNumber());
                	//	System.out.println("found 4 in old?"+found(4, oldPage));
                	//	System.out.println("found 4 in new?"+found(4, newPage));
                		Database.getCatalog().getDatabaseFile(pageId.getTableId()).writePage(oldPage);
                		Database.getBufferPool().getPageSet(oldPage);
                		HeapPage heapPage=(HeapPage) Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
                	//	System.out.println("found 4 after writng1?"+found(4, heapPage));
                	//	System.out.println("found 4 after writng2?"+found2((HeapFile)Database.getCatalog().getDatabaseFile(pageId.getTableId()), tid,4));
                	}
                	if((typeRecord==BEGIN_RECORD)&&(transactionIDNumber==tid.getId())) {
                		break;
                	}
                	raf.seek(lastRecordOffset-8);
                	lastRecordOffset=raf.readLong();
     
				}
                
            }
        }
        raf.seek(raf.length());
    }
    boolean found(int v1,HeapPage page) {
    	boolean isFound=false;
    	for(int i=0;i<page.numSlots;++i) {
    		if(page.isSlotUsed(i)) {
    			int x = ((IntField)page.tuples[i].getField(0)).getValue();
    			if(x==v1) {
    				isFound=true;
    				break;
    			}
    		}
    	}
        return isFound;
    }
    boolean found2(HeapFile hf, TransactionId t, int v1) {
    	boolean isFound=false;
    	SeqScan scan = new SeqScan(t, hf.getId(), "");
        try {
			scan.open();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        try {
			while(scan.hasNext()){
			    Tuple tu = scan.next();
			    int x = ((IntField)tu.getField(0)).getValue();
			    if(x == v1) {
			    	isFound=true;
			    	break;
			    }
			        
			}
		} catch (NoSuchElementException | TransactionAbortedException | DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        scan.close();
        return isFound;
    }
    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // some code goes here
                /*HashMap<Long, Long> undoList=new HashMap<>();
                HashMap<Long, Boolean> redoCommitList=new HashMap<>();
                HashMap<Long, Boolean> redoAbortList=new HashMap<>();*/
                
                HashMap<Long, HashMap<Integer, HeapPage> > undoList=new HashMap<>();
                HashMap<Long, HashMap<Integer, HeapPage> > redoCommitList=new HashMap<>();
                HashMap<Long, HashMap<Integer, HeapPage> > redoAbortList=new HashMap<>();
                raf.seek(0);
               long checkOffset=raf.readLong();
      //         System.out.println("checkpoint: "+checkOffset);
               long cmpValue=-1;
               //if(checkOffset!=cmpValue)
            	//  raf.seek(checkOffset);
            	/*
               while (raf.getFilePointer()<=raf.length()) {
            	   long beginOffset=raf.getFilePointer();
            	   int kind=raf.readInt();
            	   long tid=raf.readLong();
            	   switch (kind) {
				case 1:
					if(undoList.get(tid)!=null) {
						redoAbortList.put(tid, true);
						undoList.put(tid, null);
					}
					raf.readLong();
					break;
				case 2:
					if(undoList.get(tid)!=null) {
						redoCommitList.put(tid, true);
						undoList.put(tid, null);
					}
					raf.readLong();
					break;
				case 3:
					undoList.put(tid, beginOffset);
					readPageData(raf);
					readPageData(raf);
					raf.readLong();
					break;
				case 4:
					raf.readLong();
					break;
				
				default:
					break;
				}
			}*/
            long beginOffset;
            if(checkOffset!=cmpValue)
            	beginOffset=checkOffset;
            else
            	beginOffset=8;
            beginOffset=8;
   //         System.out.println(raf.length());
             raf.seek(raf.length()-8);
            long lastOffset=raf.readLong();
   //         System.out.println("lastoffset:"+lastOffset);
            while (lastOffset>=beginOffset) {
            	raf.seek(lastOffset);
   //         	System.out.println("currentOffset: "+lastOffset);
				int type=raf.readInt();
				long tid=raf.readLong();
	//			System.out.println("type:"+type+" tid: "+tid);
				switch (type) {
				case 1:
					redoAbortList.put(tid, new HashMap<>());
					break;
				case 2:
					redoCommitList.put(tid, new HashMap<>());
					break;
				case 3:
					Page oldPage=readPageData(raf);
					Page newPage=readPageData(raf);
					if(redoAbortList.get(tid)==null&&redoCommitList.get(tid)==null) {
						if(undoList.get(tid)==null)
							undoList.put(tid, new HashMap<>());
						undoList.get(tid).put(oldPage.getId().hashCode(),(HeapPage) oldPage);
					}
					else {
						if(redoCommitList.get(tid)!=null) {
		//					System.out.println(newPage.getId().hashCode()+""+(redoCommitList.get(tid).get(newPage.getId().hashCode())==null));
							if(redoCommitList.get(tid).get(newPage.getId().hashCode())==null) {
								redoCommitList.get(tid).put(newPage.getId().hashCode(), (HeapPage) newPage);
		//						System.out.println("tid:"+tid+" page:"+newPage.getId().hashCode()+" "+(redoCommitList.get(tid).get(newPage.getId().hashCode())==null));
							}
								
							
						}
						else {
							redoAbortList.get(tid).put(oldPage.getId().hashCode(), (HeapPage) oldPage);
						}
					}	
					break;
				case 5:
					int numRecords=raf.readInt();
					for(int i=0;i<numRecords;++i) {
						raf.readLong();
						raf.readLong();
					}
					break;
				default:
					break;
				}
				
				lastOffset=raf.readLong()-8;
				if(lastOffset>beginOffset) {
					raf.seek(lastOffset);
					System.out.println(lastOffset);
					lastOffset=raf.readLong();
				}
					
				
				
				
			}
            
            Iterator iterLayer1=undoList.entrySet().iterator();
            writeForRecovering(iterLayer1);
            iterLayer1=redoAbortList.entrySet().iterator();
            writeForRecovering(iterLayer1);
            iterLayer1=redoCommitList.entrySet().iterator();
            writeForRecovering(iterLayer1);
            }
         }
    }
    private void writeForRecovering(Iterator iterLayer1) {
    	while (iterLayer1.hasNext()) {
			Map.Entry<Long, HashMap<Integer, HeapPage> > entryLayer1=(Map.Entry<Long, HashMap<Integer,HeapPage>>) iterLayer1.next();
			HashMap<Integer, HeapPage> valLayer1=entryLayer1.getValue();
			Iterator iterLayer2=valLayer1.entrySet().iterator();
			while (iterLayer2.hasNext()) {
				Map.Entry<Integer, HeapPage > entryLayer2=(Map.Entry<Integer, HeapPage>) iterLayer2.next();
				HeapPage valLayer2=entryLayer2.getValue();
				try {
					Database.getCatalog().getDatabaseFile(valLayer2.pid.getTableId()).writePage(valLayer2);
		//			System.out.println("pagenumber "+valLayer2.pid.getPageNumber());
				} catch (NoSuchElementException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        		Database.getBufferPool().getPageSet(valLayer2);
			}
		}
    }
    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        // some code goes here
    	System.out.println("print begin :");
    	raf.seek(0);
    	
    	System.out.println("checkpoint:"+raf.readLong());
    	//System.out.println("beginpoint:"+tidToFirstLogRecord.get(tId.getId()));
    	System.out.println("end:"+raf.length());
    //	long beginPoint=tidToFirstLogRecord.get(tId.getId());
    	raf.seek(raf.length()-8);
    	long endPoint=raf.readLong();
    	while (endPoint>=8) {
			raf.seek(endPoint);
			int type=raf.readInt();
			long tid=raf.readLong();
			System.out.println("@@@@@@@@@@@@@@@@@@@@@@@");
			System.out.println("type:"+type);
			System.out.println("tid:"+tid);
			raf.seek(endPoint-8);
			endPoint=raf.readLong();
			
			
    	}
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
