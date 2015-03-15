package btree;



import java.io.IOException;

import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import diskmgr.Page;

public class BTIndexPage extends BTSortedPage {

	public BTIndexPage(PageId pageno, int keytype)
			throws ConstructPageException, IOException {
		super(pageno, keytype);		
		setType(NodeType.INDEX);
	}

	public BTIndexPage(Page arg0, int arg1) throws IOException {
		super(arg0, arg1);
		setType(NodeType.INDEX);

	}

	public BTIndexPage(int arg0) throws ConstructPageException, IOException {
		super(arg0);
		setType(NodeType.INDEX);

	}

	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException {

		// It inserts a value into the index page,
		KeyDataEntry k = new KeyDataEntry(key, pageNo);
		RID rid;
		rid=super.insertRecord(k);
		return 	rid;	                    
	}

	public PageId getPageNoByKey(KeyClass key) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException{
	 
		
		KeyDataEntry entry;
	      int i;
	      
	     
		
		for (i=getSlotCnt()-1; i >= 0; i--) {
		  entry= BT.getEntryFromBytes( getpage(),getSlotOffset(i), 
					       getSlotLength(i), keyType, NodeType.INDEX);
		  
		  if (BT.keyCompare(key, entry.key) >= 0)
		    {
		      return ((IndexData)entry.data).getData();
		    }
		}
		
		return getPrevPage();
	      
	     
	      
		
		
	}

	public KeyDataEntry getFirst(RID rid) throws InvalidSlotNumberException,
			IOException, KeyNotMatchException, NodeNotMatchException,
			ConvertException {

		// super.returnRecord(arg0);

	//	rid = super.firstRecord(); // msh fahemha awi

	//	rid.copyRid(super.firstRecord());
		
		rid.pageNo=getCurPage();
		rid.slotNo=0;
		
		if(getSlotCnt()==0){return null;}
		

//		Tuple t = super.getRecord(rid);
//		byte[] arr = t.getTupleByteArray();
//		
		
		return BT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0), keyType,
				NodeType.INDEX);

	}

	public KeyDataEntry getNext(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {

		// getnext
		
		rid.slotNo++;
		int i=rid.slotNo;		
		if(rid.slotNo>=getSlotCnt()){return null;}
		
	//	rid = super.nextRecord(rid); // msh fahemha awi
	//	if(rid==null){return null;}
	//	RID newrid = super.nextRecord(rid);
	//	Tuple t = super.getRecord(newrid);
	//	byte[] arr = t.getTupleByteArray();
		return BT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType,
				NodeType.INDEX);

	}

	public PageId getLeftLink() throws IOException {

		return getPrevPage();

	}

	public void setLeftLink(PageId left) throws IOException {
		setPrevPage(left);
	}
}
