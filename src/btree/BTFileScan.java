package btree;

import java.io.IOException;
import java.util.UUID;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;

public class BTFileScan extends IndexFileScan implements GlobalConst {
	private BTLeafPage curLeafPage;
	private RID curRid;
	private KeyDataEntry curDataEnry;
	private KeyClass lower;
	private KeyClass upper;
	private BTreeHeaderPage header;

	public BTFileScan(BTreeHeaderPage headerPage, KeyClass lowKey,
			KeyClass highKey) throws IOException, ConstructPageException,
			ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, DeleteRecException {
		if (headerPage != null) {
			header = headerPage;
			lower = lowKey;
			upper = highKey;

			if (header.get_rootId().pid == INVALID_PAGE) {
				return;
			}
			if (lower == null) {
				PageId rootId = header.get_rootId();
				BTSortedPage sortedPage = new BTSortedPage(rootId,
						header.get_keyType());
				while (true) {

					if (sortedPage.getType() == NodeType.LEAF) {// base case
					// SystemDefs.JavabaseBM.unpinPage(rootId, false);
						curLeafPage = new BTLeafPage(rootId,
								header.get_keyType());
						curRid = new RID();
						if (curLeafPage.firstRecord() != null) {// DONT FORGET
																// <<<<<<<<<<<<<<<<<<<<<<<<<<
							curRid.copyRid(curLeafPage.firstRecord());
							break;
							// curRid.slotNo = -1;

						}

					}
					SystemDefs.JavabaseBM.unpinPage(rootId, false);
					sortedPage = new BTSortedPage(sortedPage.getPrevPage(),
							header.get_keyType());
					rootId=sortedPage.getCurPage();


				}
			}else{
			locationAtLeafOfKey(headerPage.get_rootId(), lower);
			
		}}

	}
	private void locationAtLeafOfKey(PageId curPid , KeyClass keyData) throws ConstructPageException, IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, DeleteRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException{
		  
		  BTSortedPage curPage = new BTSortedPage(curPid, header.get_keyType());
		  //SystemDefs.JavabaseBM.unpinPage(curPid, false);
		  if (curPage.getType() == NodeType.INDEX) {
			  
		   BTIndexPage indexTemp = new BTIndexPage(curPid, header.get_keyType());
		   RID rid = new RID();
		   KeyDataEntry temp = indexTemp.getFirst(rid);
		   
		   if (temp == null) {
		    if(indexTemp.getPrevPage().pid == -1) return ;
		    
		    locationAtLeafOfKey(indexTemp.getPrevPage(), keyData);
		   }
		   else if (BT.keyCompare(keyData, temp.key) == 0){
			   locationAtLeafOfKey( ((IndexData)temp.data).getData(), keyData);
		   }
		   else if (BT.keyCompare(keyData, temp.key) < 0){
			   locationAtLeafOfKey(indexTemp.getPrevPage(), keyData);
		    
		   }
		   else{
		   KeyDataEntry prev = null;
		   while ( temp != null){
		    prev = temp;
		   

		        temp = indexTemp.getNext(rid);
		        if (temp == null) {
		        	locationAtLeafOfKey(((IndexData) prev.data).getData(),keyData);
		           
		        } else {
		         int d = BT.keyCompare(keyData, temp.key);
		         if (d < 0) {
		        	 locationAtLeafOfKey( ((IndexData) prev.data).getData(),keyData);
		         } else if (d == 0) {
		        	 locationAtLeafOfKey( ((IndexData) temp.data).getData(),keyData);
		         }
		         
		        }
		   }}
		   
		  }
		  else {
			  curLeafPage = new BTLeafPage(curPid, header.get_keyType());
				curRid = new RID();
				if (curLeafPage.firstRecord() != null) {// DONT FORGET
														// <<<<<<<<<<<<<<<<<<<<<<<<<<
					curRid.copyRid(curLeafPage.firstRecord());

					// curRid.slotNo = -1;

				
					KeyDataEntry entry;

					while (true) {
						entry = curLeafPage.getCurrent(curRid);
						
						if (BT.keyCompare(entry.key, lower) == 0) {
							break;
						}
						curRid.slotNo++;
					}
				}
				return;
		  }
		  
		  
		 }

	private void locAtLeafPage(PageId rootId) throws ConstructPageException,
			IOException, ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {

		BTSortedPage sortedPage = new BTSortedPage(rootId, header.get_keyType());

		if (sortedPage.getType() == NodeType.LEAF) {// base case
			SystemDefs.JavabaseBM.unpinPage(rootId, false);
			curLeafPage = new BTLeafPage(rootId, header.get_keyType());
			curRid = new RID();
			if (curLeafPage.firstRecord() != null) {// DONT FORGET
													// <<<<<<<<<<<<<<<<<<<<<<<<<<
				curRid.copyRid(curLeafPage.firstRecord());

				// curRid.slotNo = -1;

			
				KeyDataEntry entry;

				while (true) {
					entry = curLeafPage.getCurrent(curRid);
					if (BT.keyCompare(entry.key, lower) == 0) {
						break;
					}
				}
			}

			return;

		}
		SystemDefs.JavabaseBM.unpinPage(rootId, false);
		BTIndexPage indexPage = new BTIndexPage(rootId, header.get_keyType());
		PageId newRootId;
		if (lower == null) {
			curRid = indexPage.firstRecord();

			KeyDataEntry entry = indexPage.getFirst(curRid);
			newRootId = ((IndexData) entry.data).getData();
		} else {
			newRootId = indexPage.getPageNoByKey(lower);
		}

		SystemDefs.JavabaseBM.unpinPage(rootId, false);
		locAtLeafPage(newRootId);
	}

	// private void locAtLeafPage(PageId rootId) throws ConstructPageException,
	// IOException, ReplacerException, PageUnpinnedException,
	// HashEntryNotFoundException, InvalidFrameNumberException,
	// InvalidSlotNumberException, KeyNotMatchException,
	// NodeNotMatchException, ConvertException {
	//
	// BTSortedPage sortedPage = new BTSortedPage(rootId, header.get_keyType());
	//
	// if (sortedPage.getType() == NodeType.LEAF) {// base case
	// SystemDefs.JavabaseBM.unpinPage(rootId, false);
	// curLeafPage = new BTLeafPage(rootId, header.get_keyType());
	// curRid = new RID();
	// if (curLeafPage.firstRecord() != null) {// DONT FORGET
	// // <<<<<<<<<<<<<<<<<<<<<<<<<<
	// curRid.copyRid(curLeafPage.firstRecord());
	//
	// // curRid.slotNo = -1;
	//
	// if (lower == null) {
	// return;
	// }
	// KeyDataEntry entry;
	//
	// while (true) {
	// entry = curLeafPage.getCurrent(curRid);
	// if (BT.keyCompare(entry.key, lower) == 0) {
	// break;
	// }
	// }
	// }
	//
	// return;
	//
	// }
	// SystemDefs.JavabaseBM.unpinPage(rootId, false);
	// BTIndexPage indexPage = new BTIndexPage(rootId, header.get_keyType());
	// PageId newRootId;
	// if (lower == null) {
	// curRid =indexPage.firstRecord();
	//
	// KeyDataEntry entry = indexPage.getFirst(curRid);
	// newRootId = ((IndexData) entry.data).getData();
	// } else {
	// newRootId = indexPage.getPageNoByKey(lower);
	// }
	//
	// SystemDefs.JavabaseBM.unpinPage(rootId, false);
	// locAtLeafPage(newRootId);
	// }

	@Override
	public KeyDataEntry get_next() throws ConstructPageException, IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, ReplacerException,
			PageUnpinnedException, HashEntryNotFoundException,
			InvalidFrameNumberException {
		KeyDataEntry retEntry = null;
		if (curLeafPage != null) {

			while (true) {
				retEntry = curLeafPage.getCurrent(curRid);
				curRid.slotNo++;
				if (retEntry != null)
					break;
				// if (curRid == null) {
				PageId nextLeaf = curLeafPage.getNextPage();
				if (nextLeaf.pid == INVALID_PAGE)
					return curDataEnry = null;

				SystemDefs.JavabaseBM
						.unpinPage(curLeafPage.getCurPage(), false);
				curLeafPage = new BTLeafPage(nextLeaf, header.get_keyType());
				curRid.copyRid(curLeafPage.firstRecord());

				// }
			}
			if (upper != null) {
				if (BT.keyCompare(retEntry.key, upper) > 0) {
					return curDataEnry = null;
				}
			}

		}

		return curDataEnry = retEntry;
	}

	@Override
	public void delete_current() throws DeleteRecException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, IOException,
			HashOperationException, PageUnpinnedException, PagePinnedException,
			PageNotFoundException, BufMgrException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			LeafDeleteException {
		if (curDataEnry != null) {
			curLeafPage.delEntry(curDataEnry);
			PageId pgid = curLeafPage.getCurPage();
			SystemDefs.JavabaseBM.unpinPage(pgid, true);

			SystemDefs.JavabaseBM.pinPage(pgid, curLeafPage, false);
			if (curRid.slotNo != 0) {
				curRid.slotNo--;
			}
			curDataEnry = null;
		}

	}

	@Override
	public int keysize() throws IOException {

		return header.getmaxKeySize();
	}

	public void DestroyBTreeFileScan() throws ReplacerException,
			PageUnpinnedException, HashEntryNotFoundException,
			InvalidFrameNumberException, IOException {
		SystemDefs.JavabaseBM.unpinPage(curLeafPage.getCurPage(), false);
		curLeafPage = null;
		curDataEnry = null;
		curRid = null;
	}

}
