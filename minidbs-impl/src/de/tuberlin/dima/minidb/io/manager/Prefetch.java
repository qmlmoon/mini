package de.tuberlin.dima.minidb.io.manager;

import java.util.HashMap;

import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;

/**
 * a new thread for prefetch which does not wait for the I/O
 * @author qml_moon
 *
 */
public class Prefetch extends Thread {
	private Request request;
	
	private PageCache cache;
	
	private int resourceId;
	
	private FreeBuffer buffer;
	
	private HashMap<Integer, ResourceManager> resourceManager;
	
	public Prefetch(Request re, PageCache page_cache, int id, FreeBuffer free_buffer, HashMap<Integer, ResourceManager> rm) {
		request = re;
		cache = page_cache;
		resourceId = id;
		buffer = free_buffer;
		resourceManager = rm;
	}
	public void run() {
		try {
			synchronized(request) {
				request.wait();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			EvictedCacheEntry ev = cache.addPage(request.getNewPage(), resourceId);
			if (ev != null && ev.getResourceID() != -1)
				buffer.addWriteEntry(ev, resourceManager.get(ev.getResourceID()));
		} catch (CachePinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DuplicateCacheEntryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
