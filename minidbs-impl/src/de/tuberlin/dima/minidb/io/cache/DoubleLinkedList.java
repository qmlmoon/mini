package de.tuberlin.dima.minidb.io.cache;

public class DoubleLinkedList {
	
//	private byte[] binarypage;
	
	private DoubleLinkedList prev, next;
	
	private int resourceId, pageNumber;
	
	private CacheableData wrappingPage;
	
	private boolean pinned;
	
	public DoubleLinkedList(CacheableData newPage, int resourceID, boolean pinn) {
		this.prev = null;
		this.next = null;
		this.resourceId = resourceID;
		this.pageNumber = newPage.getPageNumber();
		this.wrappingPage = newPage;
		this.pinned = pinn;
	}
	
	public DoubleLinkedList(int PageNum, int resourceID, boolean pinn) {
		this.prev = null;
		this.next = null;
		this.pageNumber = PageNum;
		this.resourceId = resourceID;
		this.pinned = pinn;
	}
	
	public void addPrev(DoubleLinkedList pre) {
		this.prev = pre;
	}
	
	public DoubleLinkedList getPrev() {
		return this.prev;
	}
	
	public void addNext(DoubleLinkedList nex) {
		this.next = nex;
	}
	
	public DoubleLinkedList getNext() {
		return this.next;
	}
	
	public int getResourceId() {
		return this.resourceId;
	}
	
	public int getPageNumber() {
		return this.pageNumber;
	}
	
	public CacheableData getCacheableData() {
		return this.wrappingPage;
	}
	
	public void addPinn() {
		this.pinned = true;
	}
	
	public void unPinn() {
		this.pinned = false;
	}
}
