package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

public class BTreeIndexImpl implements BTreeIndex {
	
	private final static int INDEX_PAGE_HEADER_TYPE_OFFSET = 8;
	
	IndexSchema schema;
	BufferPoolManager bufferPool; 
	int resourceId;
	
	public BTreeIndexImpl(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {
		this.schema = schema;
		this.bufferPool = bufferPool;
		this.resourceId = resourceId;
	}

	@Override
	public IndexSchema getIndexSchema() {
		return this.schema;
	}
	
	public BTreeLeafPage getLeafPage(int pageNum) {
		CacheableData data = null;
		int headerType = -1;
		
		try {
			data = bufferPool.getPageAndPin(resourceId, pageNum);
		} catch (BufferPoolException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		
		if(headerType != BTreeLeafPage.HEADER_TYPE_VALUE) {
			//TODO: need unpin??
			bufferPool.unpinPage(resourceId, pageNum);
			return null;
		}
		
		return new BTreeLeafPage(schema, data.getBuffer());
	}
	
	public BTreeInnerNodePage getInnerNodePage(int pageNum) {
		CacheableData data = null;
		int headerType = -1;
		
		try {
			data = bufferPool.getPageAndPin(resourceId, pageNum);
		} catch (BufferPoolException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		
		if(headerType != BTreeInnerNodePage.HEADER_TYPE_VALUE) {
			//TODO: need unpin??
			bufferPool.unpinPage(resourceId, pageNum);
			return null;
		}
		
		try {
			return new BTreeInnerNodePage(schema, data.getBuffer());
		} catch (PageFormatException e) {
			return null;
		}
	}
	
	public void unpinPage(int pageNum) {
		this.bufferPool.unpinPage(resourceId, pageNum);
	}
	
	private int getPageType(CacheableData data) {
		if(data == null) return -1;
		byte[] buffer = data.getBuffer();
		return IntField.getIntFromBinary(buffer, INDEX_PAGE_HEADER_TYPE_OFFSET);
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField key)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		BTreeInnerNodePage page = null;
		//start from root
		int cur = schema.getRootPageNumber();
		int headerType = -1;
		
		do {
			try {
				data = bufferPool.getPageAndPin(resourceId, cur);
			} catch (BufferPoolException e) {
				return new RIDIterator();
			}
			headerType = this.getPageType(data);
			if(headerType == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = new BTreeInnerNodePage(schema, data.getBuffer());
			cur = page.getChildPageForKey(key);
			
			//TODO: unpin page here??
			this.unpinPage(page.getPageNumber());
			
		}while(headerType != BTreeLeafPage.HEADER_TYPE_VALUE);
		
		return new RIDIterator(this, cur, key);
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		BTreeInnerNodePage page = null;
		//start from root
		int cur = schema.getRootPageNumber();
		int headerType = -1;
		
		do {
			try {
				data = bufferPool.getPageAndPin(resourceId, cur);
			} catch (BufferPoolException e) {
				return new RIDIterator();
			}
			headerType = this.getPageType(data);
			if(headerType == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = new BTreeInnerNodePage(schema, data.getBuffer());
			cur = page.getChildPageForKey(startKey);
			
			//TODO: unpin page here??
			this.unpinPage(page.getPageNumber());
			
		}while(headerType != BTreeLeafPage.HEADER_TYPE_VALUE);
		
		return new RIDIterator(this, cur, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
	}

	@Override
	public IndexResultIterator<DataField> lookupKeys(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		BTreeInnerNodePage page = null;
		//start from root
		int cur = schema.getRootPageNumber();
		int headerType = -1;
		
		do {
			try {
				data = bufferPool.getPageAndPin(resourceId, cur);
			} catch (BufferPoolException e) {
				return new KeyIterator();
			}
			headerType = this.getPageType(data);
			if(headerType == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = new BTreeInnerNodePage(schema, data.getBuffer());
			cur = page.getChildPageForKey(startKey);
			
			//TODO: unpin page here??
			this.unpinPage(page.getPageNumber());
			
		}while(headerType != BTreeLeafPage.HEADER_TYPE_VALUE);
		
		return new KeyIterator(this, cur, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
	}

	@Override
	public void insertEntry(DataField key, RID rid) throws PageFormatException,
			IndexFormatCorruptException, DuplicateException, IOException {

		List<BTreeInnerNodePage> stack = new ArrayList<BTreeInnerNodePage>();
		
		CacheableData data = null;
		int cur = schema.getRootPageNumber();
		int headerType = -1;

		//traverse to leaf page, store the trace in stack
		do {
			try {
				data = bufferPool.getPageAndPin(resourceId, cur);
			} catch (BufferPoolException e) {
				for(BTreeInnerNodePage itr : stack) {
					this.unpinPage(itr.getPageNumber());
				}
				stack.clear();
			}
			headerType = this.getPageType(data);
			if(headerType == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			BTreeInnerNodePage page = new BTreeInnerNodePage(schema, data.getBuffer());
			stack.add(page);
			cur = page.getChildPageForKey(key);
		}while(headerType != BTreeLeafPage.HEADER_TYPE_VALUE);
		
		BTreeLeafPage oldLeaf = new BTreeLeafPage(schema, data.getBuffer());
		BTreeLeafPage newLeaf = null;
		
		//check duplicate
		if(schema.isUnique() && oldLeaf.getRIDForKey(key)!=null)
			throw new DuplicateException();
		
		if(!oldLeaf.insertKeyRIDPair(key, rid)) {
			//insert failed, start splitting
			try {
//				newLeaf = (BTreeLeafPage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.LEAF_PAGE);
				data = this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.LEAF_PAGE);
				newLeaf = new BTreeLeafPage(schema, data.getBuffer());
			} catch (BufferPoolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			//split the old leaf and move the second half elements to the new page
			newLeaf.prependEntriesFromOtherPage(oldLeaf, oldLeaf.getNumberOfEntries()/2);
			
			//insert new key/rid pair
			if(newLeaf.getFirstKey().compareTo(key) > 0) {
				oldLeaf.insertKeyRIDPair(key, rid);
			}
			else {
				newLeaf.insertKeyRIDPair(key, rid);
			}
			//update pointers and flags
			newLeaf.setLastKeyContinuingOnNextPage(oldLeaf.isLastKeyContinuingOnNextPage());
			newLeaf.setNextLeafPageNumber(oldLeaf.getNextLeafPageNumber());
			
			oldLeaf.setNextLeafPageNumber(newLeaf.getPageNumber());
			oldLeaf.setLastKeyContinuingOnNextPage(oldLeaf.getLastKey().compareTo(newLeaf.getFirstKey())==0);
			
			//update parent inner nodes
			DataField newKey = oldLeaf.getLastKey();
			int newPointer = newLeaf.getPageNumber();
			
			while(!stack.isEmpty()) {
				BTreeInnerNodePage oldInner = stack.remove(stack.size()-1);
				BTreeInnerNodePage newInner = null;
				
				if(oldInner.insertKeyPageNumberPair(newKey, newPointer)){
					//TODO: unpin or not
					this.unpinPage(oldInner.getPageNumber());
					break;
				}
				else {
					//insert failed, start splitting
					try {
//						newInner = (BTreeInnerNodePage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
						data = this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
						newInner = new BTreeInnerNodePage(schema, data.getBuffer());
					} catch (BufferPoolException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return;
					}
					DataField droppedKey = oldInner.moveLastToNewPage(newInner, (oldInner.getNumberOfKeys()+1)/2 );
					if(newKey.compareTo(droppedKey) > 0) {
						newInner.insertKeyPageNumberPair(newKey, newPointer);
					}
					else {
						oldInner.insertKeyPageNumberPair(newKey, newPointer);
					}
					
					if(stack.isEmpty()) {
						//reach root node, create a new root
						BTreeInnerNodePage newRoot = null;
						try {
//							newRoot = (BTreeInnerNodePage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
							data = this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
							newRoot = new BTreeInnerNodePage(schema, data.getBuffer());
						} catch (BufferPoolException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return;
						}
						newRoot.initRootState(droppedKey, oldInner.getPageNumber(), newInner.getPageNumber());
						//update new root
						schema.setRootPageNumber(newRoot.getPageNumber());
						//TODO: unpin or not
						this.unpinPage(newRoot.getPageNumber());
						break;
					}
					else {
						newKey = droppedKey;
						newPointer = newInner.getPageNumber();
					}
				}

				//TODO: unpin or not
				this.unpinPage(oldInner.getPageNumber());
				this.unpinPage(newInner.getPageNumber());
			}
			
			
		}
		
		//TODO: unpin or not
		this.unpinPage(oldLeaf.getPageNumber());
		if(newLeaf != null) this.unpinPage(newLeaf.getPageNumber());

		for(BTreeInnerNodePage itr : stack) {
			this.unpinPage(itr.getPageNumber());
		}
		stack.clear();
	}

}
