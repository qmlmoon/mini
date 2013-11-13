package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.HashMap;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageCacheImpl;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class BufferPoolManagerImpl implements BufferPoolManager{
	
	boolean isActive = false;
	
	//<Resource ID, ResouceManager>
	HashMap<Integer, ResourceManager> resourceManagers = new HashMap<Integer, ResourceManager>();
	HashMap<PageSize, PageCache> caches = new HashMap<PageSize, PageCache>();
	Rqueue rQueue =  new Rqueue();
	Wqueue wQueues = new  Wqueue();
	HashMap<PageSize, byte[]> buffers = new HashMap<PageSize, byte[]>(); // free buffers
	

	@Override
	public void startIOThreads() throws BufferPoolException {
		isActive = true;
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeBufferPool() {
		isActive = false;
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerResource(int id, ResourceManager manager)
			throws BufferPoolException {
		
		if(!this.isActive) throw new BufferPoolException();
		if(this.resourceManagers.containsKey(id)) throw new BufferPoolException();
		
		//TODO how to get page size from Object Config
		int numPages = 100; //
		PageSize pagesize = manager.getPageSize();
		
		
		//If the buffer pool has already a cache for the page size used by that resource
		//the cache will be used for the resource. 
		//Otherwise, a new cache will be created for that page size. 
		if(this.caches.containsKey(pagesize)){
			//do nothing
		}else{
			PageCache p = new PageCacheImpl(pagesize,  numPages);
			this.caches.put(pagesize, p);
			byte [] b = new byte[pagesize.getNumberOfBytes()] ;
			this.buffers.put(pagesize, b );
		}
		
		this.resourceManagers.put(id, manager);
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheableData unpinAndGetPageAndPin(int resourceId,
			int unpinPageNumber, int getPageNumber) throws BufferPoolException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prefetchPage(int resourceId, int pageNumber)
			throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prefetchPages(int resourceId, int startPageNumber,
			int endPageNumber) throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId, Enum<?> type)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
}