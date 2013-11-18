package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class BufferPoolManagerImpl implements BufferPoolManager{
	
	boolean isActive = false;
	
	Config config;
	Logger logger;
	
	//<Resource ID, ResouceManager>
	HashMap<Integer, ResourceManager> resourceManagers;
	HashMap<PageSize, PageCache> caches;
	Rqueue rthread;
	WriteThread wthread;
	//write thread is merged into rthread.....cause we need to write only when buffer is full
//	Wqueue wthread = new  Wqueue();
	
	//TODO dont sure whether a cache with certain resourceId has one freebuffer or a pagesize has one freebuffer
	HashMap<PageSize,FreeBuffer> buffers; // free buffers
	

	public BufferPoolManagerImpl(Config con, Logger log) {
		this.config = con;
		this.logger = log;
		this.caches = null;
	}
	
	@Override
	public void startIOThreads() throws BufferPoolException {
		isActive = true;
		caches = new HashMap<PageSize, PageCache>();
		buffers = new HashMap<PageSize, FreeBuffer>();
		resourceManagers = new HashMap<Integer, ResourceManager>();
		rthread = new Rqueue();
		wthread = new WriteThread(this.config.getNumIOBuffers());
		rthread.start();
		wthread.start();
	}

	@Override
	public void closeBufferPool() {
		isActive = false;
		rthread.shutdown();
		for (int r:resourceManagers.keySet()) {
			for(PageSize pagesize : caches.keySet()){
				PageCache cache = caches.get(pagesize);
				FreeBuffer buffer = buffers.get(pagesize);
				CacheableData [] writeBackData = cache.getAllPagesForResource(r);
				for (CacheableData data : writeBackData) {
					buffer.addWriteEntry(new EvictedCacheEntry(data.getBuffer(), data, r), resourceManagers.get(r));
				}
			}
			
		}
		
		while(wthread.isActive()){
			
		}
		wthread.shutdown();
	}

	@Override
	public void registerResource(int id, ResourceManager manager)
			throws BufferPoolException {
		if(!this.isActive) throw new BufferPoolException();
		if(this.resourceManagers.containsKey(id)) throw new BufferPoolException();
		
	
		PageSize pagesize = manager.getPageSize();
		
		int numPages = this.config.getCacheSize(pagesize); //
		
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);
		//If the buffer pool has already a cache for the page size used by that resource
		//the cache will be used for the resource. 
		//Otherwise, a new cache will be created for that page size. 
		if(cache != null){
			//do nothing
		}else{
			cache = AbstractExtensionFactory.getExtensionFactory().createPageCache(pagesize,  numPages);
			caches.put(pagesize, cache);
			buffer = new FreeBuffer(pagesize.getNumberOfBytes(), this.config.getNumIOBuffers(), rthread, wthread) ;
			buffers.put(pagesize, buffer);
		}
		
		this.resourceManagers.put(id, manager);
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber)
			throws BufferPoolException, IOException {
		
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		//lock the cache
		synchronized(cache) {
//			System.out.println("pin" + resourceId + " " + pageNumber);

			//if page hit in cache
			CacheableData tmp = cache.getPageAndPin(resourceId, pageNumber);
			if (tmp != null) {
//				System.out.println("readf" + resourceId + " " + pageNumber);
				return tmp;
			}
			else {
				Request r = new Request(resourceId, pageNumber, this.resourceManagers.get(resourceId), buffer);

				//enqueue the request
				//wait

				try {

					synchronized(r) {
//						System.out.println("im wait" + resourceId + " " + pageNumber);
						rthread.enQueue(r);
						r.wait();
//						System.out.println("im not wait" + resourceId + " " + pageNumber);

					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//add the new page to cache + add the evicted to freebuffer
				try {
					EvictedCacheEntry ev = cache.addPageAndPin(r.getNewPage(), resourceId);
					if (ev != null && ev.getResourceID() != -1) 
						buffer.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
				} catch (CachePinnedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DuplicateCacheEntryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return r.getNewPage();
			}
		}
	}

	@Override
	public CacheableData unpinAndGetPageAndPin(int resourceId,
			int unpinPageNumber, int getPageNumber) throws BufferPoolException,
			IOException {
		//just put unpin and getpageandpin together and locked together
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		synchronized(cache) {
			cache.unpinPage(resourceId, unpinPageNumber);
			CacheableData tmp = cache.getPageAndPin(resourceId, getPageNumber);
			if (tmp != null)
				return tmp;
			else {
				Request r = new Request(resourceId, getPageNumber, this.resourceManagers.get(resourceId), buffer);
				rthread.enQueue(r);
				try {
					synchronized(r) {
						r.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					EvictedCacheEntry ev = cache.addPageAndPin(r.getNewPage(), resourceId);
					if (ev != null && ev.getResourceID() != -1) 
						buffer.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
				} catch (CachePinnedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DuplicateCacheEntryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return r.getNewPage();
			}
		}
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
//		System.out.println("unpin" + resourceId + " " + pageNumber);

		if(!this.isActive) return;
		if(!this.resourceManagers.containsKey(resourceId)) return;
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		synchronized(cache) {
			cache.unpinPage(resourceId, pageNumber);
		}
		
	}

	@Override
	public void prefetchPage(int resourceId, int pageNumber)
			throws BufferPoolException {

		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		synchronized(cache) {
			CacheableData tmp = cache.getPage(resourceId, pageNumber);
			if (tmp != null)
				return;
			else {
				//TODO cause this function return immediately dont wait, create a new thread to wait. any better idea?
				Request r = new Request(resourceId, pageNumber, this.resourceManagers.get(resourceId), buffer);
				Prefetch pf = new Prefetch(r, cache, resourceId, buffer, this.resourceManagers, rthread);
				pf.start();
				
			}
		}		
	}

	@Override
	public void prefetchPages(int resourceId, int startPageNumber,
			int endPageNumber) throws BufferPoolException {
		
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		synchronized(cache) {
			for (int i = startPageNumber; i <= endPageNumber; i++) {
				CacheableData tmp = cache.getPage(resourceId, i);
				if (tmp == null) {
					Request r = new Request(resourceId, i, this.resourceManagers.get(resourceId), buffer);
					Prefetch pf = new Prefetch(r, cache, resourceId, buffer, this.resourceManagers, rthread);
					pf.start();
				}
			}
		}
		
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId)
			throws BufferPoolException, IOException {

		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		synchronized(cache) {
			CacheableData newPage = null;
			//create newpage by using resourceManager and giving freebuffer
			try {
				newPage = this.resourceManagers.get(resourceId).reserveNewPage(buffer.getReadBuffer());
			} catch (PageFormatException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				EvictedCacheEntry ev = cache.addPageAndPin(newPage, resourceId);
//				if (ev.getResourceID() != -1) 
				
				if (ev != null && ev.getResourceID() != -1) {
					buffer.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
				}
			} catch (CachePinnedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DuplicateCacheEntryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return newPage;
		}
	
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId, Enum<?> type)
			throws BufferPoolException, IOException {
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		ResourceManager rm = resourceManagers.get(resourceId);
		PageSize pagesize = rm.getPageSize();
		PageCache cache = caches.get(pagesize);
		FreeBuffer buffer = buffers.get(pagesize);

		synchronized(cache) {
			CacheableData newPage = null;
			try {
				newPage = this.resourceManagers.get(resourceId).reserveNewPage(buffer.getReadBuffer(), type);
			} catch (PageFormatException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				EvictedCacheEntry ev = cache.addPageAndPin(newPage, resourceId);
//				if (ev.getResourceID() != -1)
				if (ev != null && ev.getResourceID() != -1)
					buffer.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
			} catch (CachePinnedException e) {
				e.printStackTrace();
			} catch (DuplicateCacheEntryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return newPage;
		}
	}
	
}