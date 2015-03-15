package btree;


import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTLeafPage extends BTSortedPage {

	
	public BTLeafPage(PageId pageno,int keyType) throws ConstructPageException, IOException{
		super(pageno, keyType);		
		setType(NodeType.LEAF);		
	}
	
//pin the page with pageno, and get the corresponding BTLeafPage, also it sets the type to be NodeType.LEAF.
	
	public BTLeafPage(Page page, int keyType) throws IOException{
		super(page	, keyType);
		setType(NodeType.LEAF);
	}

	public BTLeafPage(int keyType) throws ConstructPageException, IOException{
		super(keyType);
		setType(NodeType.LEAF);
	}

	public RID insertRecord(KeyClass key,
                  RID dataRid) throws InsertRecException{
		
		KeyDataEntry k=new KeyDataEntry(key, dataRid);
		
					return super.insertRecord(k);		
		
	}
	
	
public KeyDataEntry getFirst(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException{
	
	rid.pageNo = getCurPage();
    rid.slotNo = 0; 

    if ( getSlotCnt() <= 0) {
      return null;
    }

    return BT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
			   keyType, NodeType.LEAF);	

	
}


//Iterators. One of the two functions: getFirst and getNext which provide an iterator interface to the records on a BTLeafPage.
//Parameters:
//rid - It will be modified and the first rid in the leaf page will be passed out by itself. Input and Output parameter.
//Returns:
//return the first KeyDataEntry in the leaf page. null if no more record
//o  getNext

public KeyDataEntry getNext(RID rid) throws IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException
{	
	rid.slotNo++;
	int i=rid.slotNo;		
	if(rid.slotNo>=getSlotCnt()){return null;}
	
	return BT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType,
			NodeType.LEAF);
	
}
//Iterators. One of the two functions: getFirst and getNext which provide an iterator interface to the records on a BTLeafPage.
//Parameters:
//rid - It will be modified and the next rid will be passed out by itself. Input and Output parameter.
//Returns:
//return the next KeyDataEntry in the leaf page. null if no more record.
//o  getCurrent

public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException, IOException, KeyNotMatchException, NodeNotMatchException, ConvertException
{	
	
	rid.slotNo--;
	return getNext(rid);

}

//getCurrent returns the current record in the iteration; it is like getNext except it does not advance the iterator.
//Parameters:
//rid - the current rid. Input and Output parameter. But Output=Input.
//Returns:
//return the current KeyDataEntry
//o  delEntry

public boolean delEntry(KeyDataEntry dEntry) throws DeleteRecException, IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, LeafDeleteException
{
	

    KeyDataEntry  entry;
    RID rid=new RID(); 
    
    try {
	for(entry = getFirst(rid); entry!=null; entry=getNext(rid)) 
	  {  
	    if ( entry.equals(dEntry) ) {
	      if ( super.deleteSortedRecord( rid ) == false )
		throw new LeafDeleteException(null, "Delete record failed");
	      return true;
	    }
	    
	 }
	return false;
    } 
    catch (Exception e) {
	throw new LeafDeleteException(e, "delete entry failed");

    }
    	
	
	}





//delete a data entry in the leaf page.
//Parameters:
//dEntry - the entry will be deleted in the leaf page. Input parameter.
//Returns:
//true if deleted; false if no dEntry in the page
//
//	
//	
//	
//	
	
	
	
	
}
