package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

public class BTreeFile extends IndexFile implements GlobalConst {

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String fileName;

	// BTreeFile class an index file with given filename should already exist;
	// this opens it.

	public BTreeFile(String filename) throws FileIOException,
			InvalidPageNumberException, DiskMgrException, IOException,
			ReplacerException, HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException {
		fileName = filename;
		headerPageId = SystemDefs.JavabaseDB.get_file_entry(filename);
		if (headerPageId!=null) {
			headerPage = new BTreeHeaderPage(headerPageId);
		}
		

	}

	// if index file exists, open it; else create it.
	public BTreeFile(String filename, int keyType, int keySize,
			int delete_fashion) throws FileIOException,
			InvalidPageNumberException, DiskMgrException, IOException,
			BufferPoolExceededException, HashOperationException,
			ReplacerException, HashEntryNotFoundException,
			InvalidFrameNumberException, PagePinnedException,
			PageUnpinnedException, PageNotReadException, BufMgrException,
			FileNameTooLongException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException {
		fileName = filename;
		headerPageId = SystemDefs.JavabaseDB.get_file_entry(filename);
		if (headerPageId == null) {
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getCurPage();

			SystemDefs.JavabaseDB.add_file_entry(filename, headerPageId);

			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keyType);
			headerPage.set_maxKeySize(keySize);
			headerPage.setType(NodeType.BTHEAD);

		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

	}

	public void close() throws ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException {
		// check that the file is opened
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}

	}

	public void destroyFile() throws IOException, ConstructPageException,
			ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			InvalidBufferException, HashOperationException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, DiskMgrException,
			FileEntryNotFoundException, FileIOException,
			InvalidPageNumberException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException {
		// check that the file is opened
		if (headerPage != null) {
			PageId root = headerPage.get_rootId();
			if (root.pid != INVALID_PAGE) {
				destroyFile(root);
			}
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			SystemDefs.JavabaseBM.freePage(headerPageId);
			SystemDefs.JavabaseDB.delete_file_entry(fileName);
			headerPage = null;
		}
	}

	private void destroyFile(PageId pageId) throws ConstructPageException,
			IOException, ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			InvalidBufferException, HashOperationException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, DiskMgrException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {
		BTSortedPage sortedPage = new BTSortedPage(pageId,
				headerPage.get_keyType());
		if (sortedPage.getType() == NodeType.LEAF) {// base case
			SystemDefs.JavabaseBM.unpinPage(pageId, false);
			SystemDefs.JavabaseBM.freePage(pageId);

			BTIndexPage indexPage = new BTIndexPage(pageId,
					headerPage.get_keyType());
			RID rid = new RID();
			KeyDataEntry entry = indexPage.getFirst(rid);
			while (entry != null) {
				destroyFile(((IndexData) entry.data).getData());
				entry = indexPage.getNext(rid);
			}

		}
	}

	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	// create a scan with given keys Cases:

	// (1) lo_key = null, hi_key = null scan the whole index
	// (2) lo_key = null, hi_key!= null range scan from min to the hi_key
	// (3) lo_key!= null, hi_key = null range scan from the lo_key to max
	// (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match ( might not
	// unique)
	// (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key
	// to hi_key

	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey)
			throws ConstructPageException, ReplacerException,
			PageUnpinnedException, HashEntryNotFoundException,
			InvalidFrameNumberException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			IOException, DeleteRecException {

		return new BTFileScan(headerPage, lowkey, hikey);

	}
	
		
	
	public void insert(KeyClass key, RID rid) throws ConstructPageException, IOException, InsertRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, KeyNotMatchException, NodeNotMatchException, InvalidSlotNumberException, ConvertException, DeleteRecException, InsertException, DiskMgrException, KeyTooLongException {

		 // if root is empty		
		
		  
	      if (BT.getKeyLength(key) > headerPage.getmaxKeySize())
		throw new KeyTooLongException(null,"");
	      
	      if ( key instanceof StringKey ) {
		if ( headerPage.get_keyType() != AttrType.attrString ) {
		  throw new KeyNotMatchException(null,"");
		}
	      }
	      else if ( key instanceof IntegerKey ) {
		if ( headerPage.get_keyType() != AttrType.attrInteger ) {
		  throw new KeyNotMatchException(null,"");
		}
	      }   
	      else {
		throw new KeyNotMatchException(null,"");}
		
		
		if(headerPage.get_rootId().pid==INVALID_PAGE){
			
			PageId rootpageId=new PageId();
		BTLeafPage rootpage=new BTLeafPage(headerPage.get_keyType());	
			rootpageId=rootpage.getCurPage();
			
			 rootpage.setNextPage(new PageId(INVALID_PAGE));
			
			rootpage.setPrevPage(new PageId(INVALID_PAGE));
			
			rootpage.insertRecord(key, rid);
			
			SystemDefs.JavabaseBM.unpinPage(rootpageId, true); /* = DIRTY */

			
			// 11111			
			
		headerPage.set_rootId(rootpageId);
			
		//  updateHeader(rootpageId);	  
			  
			
			return;
		}					  
		 			 
		
	KeyDataEntry newRoot= recursiveInsert(key, rid,headerPage.get_rootId());
		
	// if root !=null then split wasal l7ad el root then change header page 
	// else if root == null then tmam
     
     if (newRoot != null)
	{
	 
    BTIndexPage newRootPage;
	  PageId      newRootPageId;
	  
	  newRootPage = new BTIndexPage(headerPage.get_keyType());
	  newRootPageId=newRootPage.getCurPage();
	  
	  newRootPage.insertKey( newRoot.key, ((IndexData)newRoot.data).getData() );		  
	  
	  // the old root split and is now the left child of the newroot
	  
	  newRootPage.setPrevPage(headerPage.get_rootId());
	  
	  SystemDefs.JavabaseBM.unpinPage(newRootPageId, true);		  
	   		               

  //   header.set_rootId( newRootPageId);
	  
 headerPage.set_rootId(newRootPageId);
     
     //	      
//     SystemDefs.JavabaseBM.unpinPage(headerPageID, true );
//	    
	  
	}
       
     return;
   		}		
			
	


	private KeyDataEntry recursiveInsert(KeyClass key, RID rid,
			PageId currentPageId) throws IOException, KeyNotMatchException, NodeNotMatchException, InsertRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, ConstructPageException, InvalidSlotNumberException, ConvertException, DeleteRecException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InsertException {

		Page page ;
	      KeyDataEntry copyupEntry;      
	      BTSortedPage currentPage ;
	        
	      
	      
	      Page page3=new Page();
	    SystemDefs.JavabaseBM.pinPage(currentPageId, page3, false);
	     
	    page=page3;
	      
	  currentPage=new BTSortedPage(page, headerPage.get_keyType());      
//       
//	System.out.println("head "+ NodeType.BTHEAD);
//	System.out.println(NodeType.INDEX);
//	System.out.println(NodeType.LEAF);
//	
//	
//	    System.out.println(currentPage.getType());
//	    
	    // if leaf 
	    // if index 
	    
	    if(currentPage.getType()==NodeType.LEAF){        // law hia leaf w feha mkan
	    	
	    //	System.out.println("dfsdfsdfs");
	    	
	    	  BTLeafPage currentLeafPage=new BTLeafPage(page, headerPage.get_keyType() );

	    	  PageId currentLeafPageId = currentPageId;
	    		    	  
	    	  // check whether there can still be entries inserted on that page
	        	  
	    	  if (currentLeafPage.available_space() >=
	    	      BT.getKeyDataLength(key, NodeType.LEAF) )
	    	    {
	    	      // no split has occurred
	    	      
	    	      currentLeafPage.insertRecord(key, rid); 
	    	      
	    	     SystemDefs.JavabaseBM.unpinPage(currentLeafPageId, true);
	    	      
	    	      return null;   	    	
	    	    }
	   // law hia leaf w mafesh mkan fady split    	
	    	 // allocate new leaf page w redistribute
	    	  
	    	  BTLeafPage  newLeafPage;
	    	  PageId       newLeafPageId;
	    	  
	    	  newLeafPage=new BTLeafPage(headerPage.get_keyType());
	    	  newLeafPageId=newLeafPage.getCurPage();
	    	  
	    	  newLeafPage.setNextPage(currentLeafPage.getNextPage());
	    	  newLeafPage.setPrevPage(currentLeafPageId);  // cause its double linked list
	   
	    	  currentLeafPage.setNextPage(newLeafPageId);
	    	  	  
	    	  PageId PageId;
	    	   PageId = newLeafPage.getNextPage();
	    	  
	    	  if (PageId.pid != INVALID_PAGE)  // law kan feh asln leafpage b3deha
	    	    {
	    	      BTLeafPage rightPage;
	    	      rightPage=new BTLeafPage(PageId, headerPage.get_keyType());	    	      
	    	      rightPage.setPrevPage(newLeafPageId);
	    	      SystemDefs.JavabaseBM.unpinPage(PageId, true );
	    	          	    }
	    	  
	    	  // then redestribute
	    	  
	    	  KeyDataEntry     tmpEntry;
	    	  RID       firstRid=new RID();
	    	  
	    	  
	    	  for (tmpEntry = currentLeafPage.getFirst(firstRid);tmpEntry != null; tmpEntry = currentLeafPage.getFirst(firstRid))   // msh fahem
	    	    {	    	      
	    	      newLeafPage.insertRecord( tmpEntry.key,((LeafData)(tmpEntry.data)).getData());
	    	      currentLeafPage.deleteSortedRecord(firstRid);
	    	     	    	    }
	    	  
	    	  
	    	  KeyDataEntry tempEntry=null; 
	    	  
	         for (tmpEntry = newLeafPage.getFirst(firstRid);newLeafPage.available_space() < currentLeafPage.available_space(); tmpEntry=newLeafPage.getFirst(firstRid)   )
	    	    {	   
	    	      tempEntry=tmpEntry;
	    	      currentLeafPage.insertRecord( tmpEntry.key,((LeafData)tmpEntry.data).getData());
	    	      newLeafPage.deleteSortedRecord(firstRid);		
	    	    }    	  
	    	// keda 5las et2smo bel nos
	    	     	  	    	  
	    	  
	    	 // law mthln  123   5    law 7atinsert less than 3 then move 3 to new leafpage 
	    	  
	    	  if (BT.keyCompare(key, tempEntry.key ) <  0) {
	    	    
	    	    if ( currentLeafPage.available_space() < newLeafPage.available_space()) {
	    	      newLeafPage.insertRecord( tempEntry.key, 
	    					((LeafData)tempEntry.data).getData());
	    	      
	    	      currentLeafPage.deleteSortedRecord
	    		(new RID(currentLeafPage.getCurPage(),
	    			 (int)currentLeafPage.getSlotCnt()-1) );              
	    	    }
	    	  }	  
	    	   
	    	  
	    	  // else 123  5  insert bigger than 3 then insert int new leaf page 3latol
	    	  
	    	  
	    	  if (BT.keyCompare(key,tempEntry.key ) >= 0)
	  	    {                     
	  	      newLeafPage.insertRecord(key, rid);
	  	     
	  	    }
	  	  else {
	  	    currentLeafPage.insertRecord(key,rid);
	  	  }
	  	  
	  	  SystemDefs.JavabaseBM.unpinPage(currentLeafPageId, true );
	  	 
	  	  // copy up cause its a leaf splitting
	  	  
	  	  tmpEntry=newLeafPage.getFirst(firstRid);
	  	  copyupEntry=new KeyDataEntry(tmpEntry.key, newLeafPageId );
	  	  
	  	  
	  	SystemDefs.JavabaseBM.unpinPage(newLeafPageId, true);
	  	  
	  	  return copyupEntry;
	  	}
	    
	    ////////////////////////////////////////////////////////////////// insert at index 
	    
	    else if (currentPage.getType()==NodeType.INDEX){
	    		    	  
	    //	System.out.println("wedfasdfadsfasdf");
	    	
	    	
	    	BTIndexPage  currentIndexPage=new BTIndexPage(page, headerPage.get_keyType());
	    	
	    	PageId       currentIndexPageId = currentPageId;
	    	
	    	PageId nextPageId;
	    	
	    	nextPageId=currentIndexPage.getPageNoByKey(key);
	    	
	    	SystemDefs.JavabaseBM.unpinPage(currentIndexPageId,false);  
	    	
	    	copyupEntry= recursiveInsert(key, rid, nextPageId);
	    	
	    		    	
	    	if ( copyupEntry == null)   // law copy up ==null then no split has happened ok
	    	  return null;
	    	                              
	    	                                // else split has happened and copy up fiha elly 7ait7at f el father
	    	Page page2=new Page();
	    	
	    	SystemDefs.JavabaseBM.pinPage(currentPageId,page2,false);
	    	
	    	currentIndexPage= new  BTIndexPage(page2, headerPage.get_keyType() );
	    	
	    	// law lessa feha mkan yb2a 7lal mn 3'er split
	    	
	    	if (currentIndexPage.available_space() >= BT.getKeyDataLength( copyupEntry.key, NodeType.INDEX))
	    	  { 	    
	    	    
	    	    currentIndexPage.insertKey( copyupEntry.key, ((IndexData)copyupEntry.data).getData() );
	    	    
	    	   SystemDefs.JavabaseBM.unpinPage(currentIndexPageId, true);
	    	    
	    	   return null;
	    	  }
	    	
	    	/// else then split and allocate new index page and redistribute
	    	
	    	BTIndexPage newIndexPage;
	    	PageId       newIndexPageId;
	    	
	    	newIndexPage= new BTIndexPage(headerPage.get_keyType());
	    	newIndexPageId=newIndexPage.getCurPage();  	    	
	        	    		    	
	    	KeyDataEntry      tmpEntry;	    
	    	RID deletedRid=new RID();
	    	
	    	// copy all into new index page shabh el leaf page belzabt
	    	
	    	for ( tmpEntry= currentIndexPage.getFirst( deletedRid);tmpEntry!=null;tmpEntry= currentIndexPage.getFirst( deletedRid))  
	    	  {
	    	    newIndexPage.insertKey( tmpEntry.key,((IndexData)tmpEntry.data).getData());
	    	    currentIndexPage.deleteSortedRecord(deletedRid);
	    	  }
	    	
	    	//  copy back to currentindex page until equal
	    	
	    	RID firstRid=new RID();
	    	KeyDataEntry tempEntry=null;
	    	for (tmpEntry = newIndexPage.getFirst(firstRid);(currentIndexPage.available_space() >newIndexPage.available_space());tmpEntry=newIndexPage.getFirst(firstRid))
	    	  {	    	    
	    	    tempEntry=tmpEntry;
	    	    currentIndexPage.insertKey( tmpEntry.key, ((IndexData)tmpEntry.data).getData());
	    	    newIndexPage.deleteSortedRecord(firstRid);
	              }
	    	
	    // shabah el leaf belzabt
	    	if ( currentIndexPage.available_space() <newIndexPage.available_space()) {
	    	  
	    	  newIndexPage.insertKey( tempEntry.key,((IndexData)tempEntry.data).getData());
	    	  
	    	  currentIndexPage.deleteSortedRecord (new RID(currentIndexPage.getCurPage(),(int)currentIndexPage.getSlotCnt()-1) );              
	    	}
	    	
	    	
	    	// see where to put the new intery (copyuped entry) shabah el leaf
	    	
	    	tmpEntry= newIndexPage.getFirst(firstRid);
	    	
	    	if (BT.keyCompare( copyupEntry.key, tmpEntry.key) >=0 )
	    	  {
	    	    
	    	    newIndexPage.insertKey( copyupEntry.key,((IndexData)copyupEntry.data).getData());
	              }
	    	else {
	    	  currentIndexPage.insertKey( copyupEntry.key,((IndexData)copyupEntry.data).getData());
	    	  
	    	  int i= (int)currentIndexPage.getSlotCnt()-1;
	    	  tmpEntry = BT.getEntryFromBytes(currentIndexPage.getpage(),currentIndexPage.getSlotOffset(i), currentIndexPage.getSlotLength(i),headerPage.get_keyType(),NodeType.INDEX);

	    	  newIndexPage.insertKey( tmpEntry.key, ((IndexData)tmpEntry.data).getData());

	    	  currentIndexPage.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), i) );      
	    	  
	    	}
	    	
	    	
	    	  SystemDefs.JavabaseBM.unpinPage(currentIndexPageId, true );
	            
	    	  // pushup l2n da index again l7ad manwsal ll root
	    	  
	    	  copyupEntry= newIndexPage.getFirst(deletedRid);

	    	  // nzabat el pointers
	    	  
	    	  newIndexPage.setPrevPage( ((IndexData)copyupEntry.data).getData());

	    	  // delete first l2no push up
	    	  
	    	  newIndexPage.deleteSortedRecord(deletedRid);
	    	  
	    	  SystemDefs.JavabaseBM.unpinPage(newIndexPageId, true);	    	  
	    	
	          ((IndexData)copyupEntry.data).setData( newIndexPageId);
	    	  
	              return copyupEntry;  
	     	
	    }
	      
	    
	        else {    
	  	  throw new InsertException(null,"");
	        }
	    	    	  
	    }
		
////////////////////////////////////////////////////////////////////////////////////////		
		
		

	

	public void traceFilename(String string) {
		// TODO Auto-generated method stub
		
	}

	

//	delete leaf entry given its pair
	public boolean Delete(KeyClass key, RID rid) throws IteratorException, KeyNotMatchException, ConstructPageException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException, DeleteRecException, LeafDeleteException {
	  	     
		 RID curRid=new RID(); 
	      PageId nextpage;
	     	      
	      BTLeafPage  leafPage=findpagecontainingKey(key, curRid);  // find first page,rid of key
	    	      
	      if( leafPage == null) return false;
	      
	      KeyDataEntry  entry=leafPage.getCurrent(curRid);
	      
	      while ( true ) {
		
	        while ( entry == null) { // have to go right
		  nextpage = leafPage.getNextPage();
		  SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(),false);
		  if (nextpage.pid == INVALID_PAGE) {
		    return false;
		  }		  
		
		   Page page8=new Page();
       SystemDefs.JavabaseBM.pinPage(nextpage, page8, false);       		  
		  
		  leafPage=new BTLeafPage(   page8  ,  headerPage.get_keyType() );
		  
		  entry=leafPage.getFirst(new RID());
		}
		
		if ( BT.keyCompare(key, entry.key) > 0 )
		  break;
		
		if( leafPage.delEntry(new KeyDataEntry(key, rid)) ==true) {
		  	       
	          SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true );
		  	          return true;
		}
		
		nextpage = leafPage.getNextPage();
		SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(),false);		
	
		if(nextpage.pid==INVALID_PAGE){return false;}
		 Page page9=new Page();		 
	        SystemDefs.JavabaseBM.pinPage(nextpage, page9, false);	       
		
		leafPage=new BTLeafPage(page9 , headerPage.get_keyType());
		
		entry=leafPage.getFirst(curRid);
	      }
	      
	       
	      SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(),false);
	      return false;
	   		
	}
	
	
	
	
	private BTLeafPage findpagecontainingKey (KeyClass lo_key, RID startrid)
 throws IOException, 
	   IteratorException,  
	   KeyNotMatchException,
	   ConstructPageException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, NodeNotMatchException, ConvertException, HashEntryNotFoundException
	   {
  
		
		 BTLeafPage  pageLeaf;
	      	      Page page;      
	      	      PageId nextpageno;
	      KeyDataEntry curKey;
	      
	      PageId  pageno = headerPage.get_rootId();
	      
	      if (pageno.pid == INVALID_PAGE){       
	    //    pageLeaf = null;                1                    
	        return null;
	      }
	      
	      
	      Page page4=new Page();
	        SystemDefs.JavabaseBM.pinPage(pageno, page4, false);
	      page=page4;         
	      
	      BTSortedPage  sortPage=new BTSortedPage(page, headerPage.get_keyType());
	      	           
	      while (sortPage.getType() == NodeType.INDEX) {   // law elheader index page w feh 7agat b3deh
	    	  
	    	  BTIndexPage pageIndex=new BTIndexPage(page, headerPage.get_keyType()); 
		
	    	  PageId	prevpageno = pageIndex.getPrevPage();
	    	  
		curKey= pageIndex.getFirst(startrid);        //awel record 3la el shmal 5ales
		
		while ( curKey!=null  && BT.keyCompare(curKey.key, lo_key) < 0) {      //2
		  
	          prevpageno = ((IndexData)curKey.data).getData();
	          curKey=pageIndex.getNext(startrid);        
		}
		
		SystemDefs.JavabaseBM.unpinPage(pageno,false);		
		
		pageno = prevpageno;
		
		  Page page5=new Page();
	        SystemDefs.JavabaseBM.pinPage(pageno, page5, false);
	      
		page=page5;
		
		
		sortPage=new BTSortedPage(page, headerPage.get_keyType());   // l7ad ma nwsal lleaf page
		
				
	      }
	      
	      pageLeaf = new BTLeafPage(page, headerPage.get_keyType() );
	      
	      curKey=pageLeaf.getFirst(startrid);
	      
	      while (curKey==null) {
	  		// skip empty leaf pages
	  		nextpageno = pageLeaf.getNextPage();
	  		SystemDefs.JavabaseBM.unpinPage(pageno,false);
	  		if (nextpageno.pid == INVALID_PAGE) {
	  		  return null;
	  		}
	  		
	  		pageno = nextpageno; 		
	  		  Page page6=new Page();
	  	        SystemDefs.JavabaseBM.pinPage(pageno, page6, false);   
	  		pageLeaf=  new BTLeafPage(page6 , headerPage.get_keyType());    
	  			
	  		curKey=pageLeaf.getFirst(startrid);
	  	      }
	  	     
	      //33
	      if (lo_key == null) {
	    		return pageLeaf;
	    		
	    	      }
	    	  

	      	      
	      while (BT.keyCompare(curKey.key, lo_key) < 0) {
		curKey= pageLeaf.getNext(startrid);
		while (curKey == null) { // have to go right
		  nextpageno = pageLeaf.getNextPage();
		  SystemDefs.JavabaseBM.unpinPage(pageno,false);
		  
		  if (nextpageno.pid == INVALID_PAGE) {
		    return null;
		  }
		  
		  pageno = nextpageno;
		  Page page7=new Page();
	        SystemDefs.JavabaseBM.pinPage(pageno, page7, false);   
		  
		  pageLeaf=new BTLeafPage(page7, headerPage.get_keyType());
		  
		  curKey=pageLeaf.getFirst(startrid);
		}
	      }
	      
	      return pageLeaf;
			
 
 }
	
	
	
	
	
	
	
	
	
	
	
	

}
