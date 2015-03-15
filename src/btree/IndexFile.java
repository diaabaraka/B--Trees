package btree;

import java.io.*;

import diskmgr.DiskMgrException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import global.*;
import heap.InvalidSlotNumberException;

/**
 * Base class for a index file
 */
public abstract class IndexFile 
{
  /**
   * Insert entry into the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
 * @throws IOException 
 * @throws ConstructPageException 
 * @throws InsertRecException 
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws PageUnpinnedException 
 * @throws ReplacerException 
 * @throws BufMgrException 
 * @throws PagePinnedException 
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws HashOperationException 
 * @throws InsertException 
 * @throws DeleteRecException 
 * @throws ConvertException 
 * @throws InvalidSlotNumberException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws DiskMgrException 
 * @throws LeafDeleteException 
 * @throws IteratorException 
 * @throws IndexSearchException 
 * @throws  
 * @throws  
 * @throws IndexInsertRecException 
 * @throws LeafInsertRecException 
 * @throws KeyTooLongException 
   */
  abstract public void insert(final KeyClass data, final RID rid) throws ConstructPageException, IOException, InsertRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, KeyNotMatchException, NodeNotMatchException, InvalidSlotNumberException, ConvertException, DeleteRecException, InsertException, DiskMgrException, LeafInsertRecException, IndexInsertRecException, IteratorException, LeafDeleteException, KeyTooLongException;
  
  /**
   * Delete entry from the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
 * @throws IOException 
 * @throws HashEntryNotFoundException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws InvalidSlotNumberException 
 * @throws BufMgrException 
 * @throws PagePinnedException 
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws InvalidFrameNumberException 
 * @throws PageUnpinnedException 
 * @throws HashOperationException 
 * @throws ReplacerException 
 * @throws ConstructPageException 
 * @throws KeyNotMatchException 
 * @throws IteratorException 
 * @throws DeleteRecException 
 * @throws LeafDeleteException 
   */
  abstract public boolean Delete(final KeyClass data, final RID rid) throws IteratorException, KeyNotMatchException, ConstructPageException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException, DeleteRecException, LeafDeleteException;
}
