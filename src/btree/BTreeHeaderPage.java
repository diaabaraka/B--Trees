package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;

//Slot no. 1 -->keyType
//Slot no. 2 -->keySize
public class BTreeHeaderPage extends HFPage {

	public BTreeHeaderPage(PageId pageno) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException {
		super();
		SystemDefs.JavabaseBM.pinPage(pageno, this, false);

	}

	public BTreeHeaderPage(Page page) throws IOException {
		super(page);
	}

	public BTreeHeaderPage() throws BufferPoolExceededException,
			HashOperationException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PagePinnedException, PageUnpinnedException, PageNotReadException,
			BufMgrException, DiskMgrException, IOException {
		super();
		Page apage = new Page();
		PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
		this.init(pageId, apage);
	}

	public void set_rootId(PageId rootID) throws IOException {
		setNextPage(rootID);
	}

	public void set_keyType(short key_type) throws IOException {
		setSlot(1, (int) key_type, 0);
	}

	public void set_maxKeySize(int key_size) throws IOException {
		setSlot(2, key_size, 0);
	}

	public PageId get_rootId() throws IOException {
		return getNextPage();
	}

	public short get_keyType() throws IOException {
		return (short) getSlotLength(1);
	}

	public int getmaxKeySize() throws IOException {
		return getSlotLength(2);
	}
	public void set_deleteFashion(int fashion )
		    throws IOException
			    {
			      setSlot(3, fashion, 0);
			    }

}