package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;

public class Request{
	private int resourceId;
	private int pageNumber;
	private ResourceManager resourceManager;
	private FreeBuffer buffer;
	private CacheableData newPage;
	
	private EvictedCacheEntry ev;
	
	public Request(int resource_id, int page_number, ResourceManager resource_manager, FreeBuffer free_buffer){
		resourceId = resource_id;
		pageNumber = page_number;
		resourceManager = resource_manager;
		buffer = free_buffer;
	}
	
	public Request(int resource_id, int page_number) {
		resourceId = resource_id;
		pageNumber = page_number;
	}
	
	public Request(ResourceManager rm, EvictedCacheEntry evict) {
		resourceManager =rm;
		ev = evict;
	}
	
	public EvictedCacheEntry getEvicted() {
		return ev;
	}


	public int getResourceId(){
		return this.resourceId;
	}
	
	public int getPageNumber(){
		return this.pageNumber;
	}
	
	public ResourceManager getResourceManager() {
		return this.resourceManager;
	}
	
	public FreeBuffer getFreeBuffer() {
		return buffer;
	}
	
	public CacheableData getNewPage() {
		return newPage;
	}
	
	public void setNewPage(CacheableData new_page) {
		newPage = new_page;
	}
}