package btree;

import heap.InvalidSlotNumberException;

import java.io.IOException;

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


/**
 * Base class for a index file scan
 */
public abstract class IndexFileScan 
{
  /**
   * Get the next record.
   * @return the KeyDataEntry, which contains the key and data
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws PageUnpinnedException 
 * @throws ReplacerException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws InvalidSlotNumberException 
 * @throws IOException 
 * @throws ConstructPageException 
   */
  abstract public KeyDataEntry get_next() throws ConstructPageException, IOException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException;

  /** 
   * Delete the current record.
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws ReplacerException 
 * @throws BufMgrException 
 * @throws PageNotFoundException 
 * @throws PagePinnedException 
 * @throws PageUnpinnedException 
 * @throws HashOperationException 
 * @throws IOException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws InvalidSlotNumberException 
 * @throws DeleteRecException 
 * @throws LeafDeleteException 
   */
   abstract public void delete_current() throws DeleteRecException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, LeafDeleteException;

  /**
   * Returns the size of the key
   * @return the keysize
 * @throws IOException 
   */
  abstract public int keysize() throws IOException;
}
