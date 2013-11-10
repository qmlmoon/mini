package de.tuberlin.dima.minidb.io.cache;

public class PageCacheImpl implements PageCache {

	private DoubleLinkedList T1_head, T1_tail, T2_head, T2_tail, B1_head, B1_tail, B2_head, B2_tail;
	
	private int T1_length, T2_length, B1_length, B2_length;
	
	private int p;
	
	@Override
	public CacheableData getPage(int resourceId, int pageNumber) {
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp == T1_head) 
					T1_head = tmp.getNext();
				else {
					tmp.getPrev().addNext(tmp.getNext());
					tmp.getNext().addPrev(tmp.getPrev());
				}
				T1_length--;
				T2_head.addPrev(tmp);
				T2_head = tmp;
				T2_length++;
				return tmp.getCacheableData();
			}
		}
		
		
	    tmp = T2_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp != T2_head) {
					tmp.getPrev().addNext(tmp.getNext());
					tmp.getNext().addPrev(tmp.getPrev());
				}
				T2_head.addPrev(tmp);
				T2_head = tmp;
				return tmp.getCacheableData();
			}
		}
		
		//TODO how to get the cacheableData if this page is missing in cache
		//addPage();
		
		return null;
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber) {
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp == T1_head) 
					T1_head = tmp.getNext();
				else {
					tmp.getPrev().addNext(tmp.getNext());
					tmp.getNext().addPrev(tmp.getPrev());
				}
				T1_length--;
				T2_head.addPrev(tmp);
				T2_head = tmp;
				T2_length++;
				tmp.addPinn();
				return tmp.getCacheableData();
			}
		}
		
		
	    tmp = T2_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp != T2_head) {
					tmp.getPrev().addNext(tmp.getNext());
					tmp.getNext().addPrev(tmp.getPrev());
				}
				T2_head.addPrev(tmp);
				T2_head = tmp;
				tmp.addPinn();
				return tmp.getCacheableData();
			}
		}
		
		//TODO how to get the cacheableData if this page is missing in cache
		//addPage();
		
		return null;

	}

	@Override
	public EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CacheableData[] getAllPagesForResource(int resourceId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void expellAllPagesForResource(int resourceId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getCapacity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void unpinAllPages() {
		// TODO Auto-generated method stub
		
	}

}
