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
		
		try {
			data = bufferPool.getPageAndPin(resourceId, pageNum);
		} catch (BufferPoolException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		
		if(this.getPageType(data) != BTreeLeafPage.HEADER_TYPE_VALUE) {
			//TODO: need unpin??
			bufferPool.unpinPage(resourceId, pageNum);
			return null;
		}
		
		return (BTreeLeafPage)data;
	}
	
	public BTreeInnerNodePage getInnerNodePage(int pageNum) {
		CacheableData data = null;
		
		try {
			data = bufferPool.getPageAndPin(resourceId, pageNum);
		} catch (BufferPoolException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		
		if(this.getPageType(data) != BTreeInnerNodePage.HEADER_TYPE_VALUE) {
			//TODO: unpin or not??
			bufferPool.unpinPage(resourceId, pageNum);
			return null;
		}
		
		return (BTreeInnerNodePage)data;
	}
	
	public void prefetchPage(int pageNum) {
		try {
			this.bufferPool.prefetchPage(resourceId, pageNum);
		} catch (BufferPoolException e) {
			e.printStackTrace();
		}
	}
	
	public void unpinPage(int pageNum) {
		this.bufferPool.unpinPage(resourceId, pageNum);
	}
	
	private int getPageType(CacheableData data) {
		if(data == null) return -1;
		return IntField.getIntFromBinary(data.getBuffer(), INDEX_PAGE_HEADER_TYPE_OFFSET);
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField key)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		//start from root
		int page = schema.getRootPageNumber();
		
		do {
			data = null;
			try {
				data = bufferPool.getPageAndPin(resourceId, page);
			} catch (BufferPoolException e) {
				return new RIDIterator();
			}
			if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = ((BTreeInnerNodePage)data).getChildPageForKey(key);
			
			//TODO: unpin or not??
			this.unpinPage(data.getPageNumber());
			
		}while(data != null);

		if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
			return new RIDIterator(this, data.getPageNumber(), key);
		else
			return new RIDIterator();
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		//start from root
		int page = schema.getRootPageNumber();
		
		do {
			data = null;
			try {
				data = bufferPool.getPageAndPin(resourceId, page);
			} catch (BufferPoolException e) {
				return new RIDIterator();
			}
			if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = ((BTreeInnerNodePage)data).getChildPageForKey(startKey);
			
			//TODO: unpin page here??
			this.unpinPage(data.getPageNumber());
			
		}while(data != null);

		if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
			return new RIDIterator(this, data.getPageNumber(), startKey, stopKey, startKeyIncluded, stopKeyIncluded);
		else
			return new RIDIterator();
	}

	@Override
	public IndexResultIterator<DataField> lookupKeys(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		CacheableData data = null;
		//start from root
		int page = schema.getRootPageNumber();
		
		do {
			data = null;
			try {
				data = bufferPool.getPageAndPin(resourceId, page);
			} catch (BufferPoolException e) {
				return new KeyIterator();
			}
			if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			page = ((BTreeInnerNodePage)data).getChildPageForKey(startKey);
			
			//TODO: unpin page here??
			this.unpinPage(data.getPageNumber());
			
		}while(data != null);

		if(this.getPageType(data) == BTreeLeafPage.HEADER_TYPE_VALUE) 
			return new KeyIterator(this, page, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
		else {
			return new KeyIterator();
		}
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
			data = null;
			try {
				data = bufferPool.getPageAndPin(resourceId, cur);
			} catch (BufferPoolException e) {
				for(BTreeInnerNodePage itr : stack) {
					this.unpinPage(itr.getPageNumber());
				}
				stack.clear();
				e.printStackTrace();
				return;
			}
			headerType = this.getPageType(data);
			if(headerType == BTreeLeafPage.HEADER_TYPE_VALUE) 
				break;
			
			BTreeInnerNodePage page = (BTreeInnerNodePage)data;
			stack.add(page);
			cur = page.getChildPageForKey(key);
		}while(data != null);
		
		BTreeLeafPage oldLeaf = (BTreeLeafPage)data;
		BTreeLeafPage newLeaf = null;
		
		//check duplicate
		if(schema.isUnique() && oldLeaf.getRIDForKey(key)!=null)
			throw new DuplicateException();
		
		if(!oldLeaf.insertKeyRIDPair(key, rid)) {
			//insert failed, start splitting
			try {
				newLeaf = (BTreeLeafPage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.LEAF_PAGE);
			} catch (BufferPoolException e) {
				for(BTreeInnerNodePage itr : stack) {
					this.unpinPage(itr.getPageNumber());
				}
				stack.clear();
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
			
			if(stack.isEmpty()) {
				//reach root node, create a new root
				BTreeInnerNodePage newRoot = null;
				try {
					newRoot = (BTreeInnerNodePage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
				} catch (BufferPoolException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
				newRoot.initRootState(oldLeaf.getLastKey(), oldLeaf.getPageNumber(), newLeaf.getPageNumber());
				//update new root
				schema.setRootPageNumber(newRoot.getPageNumber());
				//TODO: unpin or not
				this.unpinPage(newRoot.getPageNumber());
			}
			else {
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
							newInner = (BTreeInnerNodePage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
						} catch (BufferPoolException e) {
							for(BTreeInnerNodePage itr : stack) {
								this.unpinPage(itr.getPageNumber());
							}
							stack.clear();
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
								newRoot = (BTreeInnerNodePage) this.bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
							} catch (BufferPoolException e) {
								for(BTreeInnerNodePage itr : stack) {
									this.unpinPage(itr.getPageNumber());
								}
								stack.clear();
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
