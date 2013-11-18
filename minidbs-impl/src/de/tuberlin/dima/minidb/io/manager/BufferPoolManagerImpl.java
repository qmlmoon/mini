package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageCacheImpl;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class BufferPoolManagerImpl implements BufferPoolManager{
	
	boolean isActive = false;
	
	Config config;
	Logger logger;
	
	//<Resource ID, ResouceManager>
	HashMap<Integer, ResourceManager> resourceManagers = new HashMap<Integer, ResourceManager>();
	PageCache cache;
	Rqueue rthread;
	WriteThread wthread;
	//write thread is merged into rthread.....cause we need to write only when buffer is full
//	Wqueue wthread = new  Wqueue();
	
	//TODO dont sure whether a cache with certain resourceId has one freebuffer or a pagesize has one freebuffer
	FreeBuffer buffers; // free buffers
	

	public BufferPoolManagerImpl(Config con, Logger log) {
		this.config = con;
		this.logger = log;
		this.cache = null;
		rthread = new Rqueue();
		wthread = new WriteThread(this.config.getNumIOBuffers());
	}
	
	@Override
	public void startIOThreads() throws BufferPoolException {
		isActive = true;
		
		rthread.start();
		wthread.start();
		System.out.println(rthread.getName());
		System.out.println(wthread.getName());
	}

	@Override
	public void closeBufferPool() {
		isActive = false;
		rthread.shutdown();
		wthread.shutdown();
	}

	@Override
	public void registerResource(int id, ResourceManager manager)
			throws BufferPoolException {
		if(!this.isActive) throw new BufferPoolException();
		if(this.resourceManagers.containsKey(id)) throw new BufferPoolException();
		
	
		PageSize pagesize = manager.getPageSize();
		
		int numPages = this.config.getCacheSize(pagesize); //
		
		
		//If the buffer pool has already a cache for the page size used by that resource
		//the cache will be used for the resource. 
		//Otherwise, a new cache will be created for that page size. 
		if(this.cache != null){
			//do nothing
		}else{
			cache = new PageCacheImpl(pagesize,  numPages);
			buffers = new FreeBuffer(pagesize.getNumberOfBytes(), this.config.getNumIOBuffers(), rthread, wthread) ;
		}
		
		this.resourceManagers.put(id, manager);
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber)
			throws BufferPoolException, IOException {
		
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		//lock the cache
		synchronized(this.cache) {
//			System.out.println("pin" + resourceId + " " + pageNumber);

			//if page hit in cache
			CacheableData tmp = this.cache.getPageAndPin(resourceId, pageNumber);
			if (tmp != null) {
//				System.out.println("readf" + resourceId + " " + pageNumber);
				return tmp;
			}
			else {
				Request r = new Request(resourceId, pageNumber, this.resourceManagers.get(resourceId), this.buffers);

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
					EvictedCacheEntry ev = this.cache.addPageAndPin(r.getNewPage(), resourceId);
					if (ev.getResourceID() != -1) 
						this.buffers.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
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
		synchronized(this.cache) {
			this.cache.unpinPage(resourceId, unpinPageNumber);
			CacheableData tmp = this.cache.getPageAndPin(resourceId, getPageNumber);
			if (tmp != null)
				return tmp;
			else {
				Request r = new Request(resourceId, getPageNumber, this.resourceManagers.get(resourceId), this.buffers);
				rthread.enQueue(r);
				try {
					synchronized(r) {
						r.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					EvictedCacheEntry ev = this.cache.addPageAndPin(r.getNewPage(), resourceId);
					if (ev.getResourceID() != -1) 
						this.buffers.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
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
		synchronized(this.cache) {
			this.cache.unpinPage(resourceId, pageNumber);
		}
		
	}

	@Override
	public void prefetchPage(int resourceId, int pageNumber)
			throws BufferPoolException {

		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		synchronized(this.cache) {
			CacheableData tmp = this.cache.getPage(resourceId, pageNumber);
			if (tmp != null)
				return;
			else {
				//TODO cause this function return immediately dont wait, create a new thread to wait. any better idea?
				Request r = new Request(resourceId, pageNumber, this.resourceManagers.get(resourceId), this.buffers);
				Prefetch pf = new Prefetch(r, this.cache, resourceId, this.buffers, this.resourceManagers, rthread);
				pf.start();
				
			}
		}		
	}

	@Override
	public void prefetchPages(int resourceId, int startPageNumber,
			int endPageNumber) throws BufferPoolException {
		
		if(!this.isActive) throw new BufferPoolException();
		if(!this.resourceManagers.containsKey(resourceId)) throw new BufferPoolException();
		synchronized(this.cache) {
			for (int i = startPageNumber; i <= endPageNumber; i++) {
				CacheableData tmp = this.cache.getPage(resourceId, i);
				if (tmp == null) {
					Request r = new Request(resourceId, i, this.resourceManagers.get(resourceId), this.buffers);
					Prefetch pf = new Prefetch(r, this.cache, resourceId, this.buffers, this.resourceManagers, rthread);
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
		synchronized(this.cache) {
			CacheableData newPage = null;
			//create newpage by using resourceManager and giving freebuffer
			try {
				newPage = this.resourceManagers.get(resourceId).reserveNewPage(this.buffers.getReadBuffer());
			} catch (PageFormatException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				EvictedCacheEntry ev = this.cache.addPageAndPin(newPage, resourceId);
//				if (ev.getResourceID() != -1) 
				
				if (ev != null) {
					this.buffers.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
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
		synchronized(this.cache) {
			CacheableData newPage = null;
			try {
				newPage = this.resourceManagers.get(resourceId).reserveNewPage(this.buffers.getReadBuffer(), type);
			} catch (PageFormatException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				EvictedCacheEntry ev = this.cache.addPageAndPin(newPage, resourceId);
//				if (ev.getResourceID() != -1)
				if (ev != null)
					this.buffers.addWriteEntry(ev, this.resourceManagers.get(ev.getResourceID()));
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