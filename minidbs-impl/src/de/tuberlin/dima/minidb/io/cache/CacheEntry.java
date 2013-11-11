package de.tuberlin.dima.minidb.io.cache;

public class CacheEntry extends EvictedCacheEntry {

	int pinned = 0;
	int hitCount = 0;
	
	boolean expelled = false;
	
	public CacheEntry(byte[] binaryPage, CacheableData wrappingPage,int resourceID) {
		super(binaryPage, wrappingPage, resourceID);
	}
	
	public CacheEntry(CacheableData wrappingPage, int resourceID) {
		super(wrappingPage.getBuffer(), wrappingPage, resourceID);
	}
	
	public CacheEntry(CacheableData wrappingPage, int resourceID, boolean pinned) {
		super(wrappingPage.getBuffer(), wrappingPage, resourceID);
		this.pinned = pinned? 1 : 0;
	}
	
	public CacheEntry(CacheableData wrappingPage, int resourceID, boolean pinned, boolean hit) {
		super(wrappingPage.getBuffer(), wrappingPage, resourceID);
		this.pinned = pinned? 1 : 0;
		this.hitCount = hit? 1 : 0;
	}
	

	public int increasePinned() {
		this.pinned++;
		return this.pinned;
	}
	
	public int decreasePinned() {
		this.pinned--;
		return this.pinned;
	}
	
	public void setUnpinned() {
		this.pinned = 0;
	}
	
	public boolean isPinned() {
		return this.pinned > 0;
	}
	
	public int increaseHit() {
		this.hitCount++;
		return this.hitCount;
	}
	public int getHitCount() {
		return this.hitCount;
	}
	
	public void markExpelled() {
		this.expelled = true;
	}
	
	public boolean isExpelled() {
		return this.expelled;
	}
	

}
