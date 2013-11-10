package de.tuberlin.dima.minidb.io.cache;

public class PageCacheImpl implements PageCache {

	private DoubleLinkedList T1_head, T1_tail, T2_head, T2_tail, B1_head, B1_tail, B2_head, B2_tail;
	
	private int T1_length, T2_length, B1_length, B2_length;
	
	private int p, c;
	
	private int pageSize;
	
	public PageCacheImpl(int pagesize, int pageNum) {
		this.p = 0;
		this.c = pageNum;
		this.pageSize = pagesize;
		T1_head = T2_head = B1_head = B2_head = T1_tail = T2_tail = B1_tail = B2_tail = null;
		T1_length = T2_length = B1_length = B2_length;
	}
	
	@Override
	public CacheableData getPage(int resourceId, int pageNumber) {
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp == T1_head) 
					T1_head = tmp.getNext();
				else {
					tmp.getPrev().addNext(tmp.getNext());
					if (tmp != T1_tail)
						tmp.getNext().addPrev(tmp.getPrev());
					else
						T1_tail = tmp.getPrev();
				}
				T1_length--;
				if (T2_length != 0) {
					T2_head.addPrev(tmp);
					tmp.addNext(T2_head);
					T2_head = tmp;
				}
				else
					T2_head = T2_tail = tmp;
				T2_length++;
				return tmp.getCacheableData();
			}
			tmp = tmp.getNext();
		}
		
		
	    tmp = T2_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp != T2_head) {
					tmp.getPrev().addNext(tmp.getNext());
					if (tmp != T2_tail)
						tmp.getNext().addPrev(tmp.getPrev());
					else 
						T2_tail = tmp.getPrev();
					T2_head.addPrev(tmp);
					tmp.addNext(T2_head);
					T2_head = tmp;
				}
				
				return tmp.getCacheableData();
			}
			tmp = tmp.getNext();
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
					if (tmp != T1_tail)
						tmp.getNext().addPrev(tmp.getPrev());
					else
						T1_tail = tmp.getPrev();
				}
				T1_length--;
				if (T2_length != 0) {
					T2_head.addPrev(tmp);
					tmp.addNext(T2_head);
					T2_head = tmp;
				}
				else
					T2_head = T2_tail = tmp;
				T2_length++;
				tmp.addPinn();
				return tmp.getCacheableData();
			}
			tmp = tmp.getNext();
		}
		
		
	    tmp = T2_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId && tmp.getPageNumber()== pageNumber) {
				if (tmp != T2_head) {
					tmp.getPrev().addNext(tmp.getNext());
					if (tmp != T2_tail)
						tmp.getNext().addPrev(tmp.getPrev());
					else 
						T2_tail = tmp.getPrev();
					T2_head.addPrev(tmp);
					tmp.addNext(T2_head);
					T2_head = tmp;
				}
				
				tmp.addPinn();
				return tmp.getCacheableData();
			}
			tmp = tmp.getNext();
		}
		//TODO how to get the cacheableData if this page is missing in cache
		//addPage();
		
		return null;

	}

	public EvictedCacheEntry subroutine_replace(CacheableData newPage, int resourceId, boolean in_B2)
		throws CachePinnedException, DuplicateCacheEntryException {
		//remove entry in T1
		if (T1_length != 0 && (T1_length > p || (T1_length == p && in_B2))) {
		
			DoubleLinkedList iter = T1_tail;
			while (iter != null && iter.isPinned())
				iter = iter.getPrev();
			if (iter != null) {
				if (iter != T1_head) {
					iter.getPrev().addNext(iter.getNext());
					if (iter != T1_tail)
						iter.getNext().addPrev(iter.getPrev());
					else
						T1_tail = iter.getPrev();
				}
				else {
					T1_head = iter.getNext();
					T1_head.addPrev(null);
				}
				T1_length--;
				B1_length++;
				DoubleLinkedList tt = new DoubleLinkedList(iter.getCacheableData().getPageNumber(), resourceId);
				if (B1_head != null) {
					B1_head.addPrev(tt);
					tt.addNext(B1_head);
					B1_head = tt;
				}
				else {
					B1_head = B1_tail = tt;
				}
				EvictedCacheEntry tmp = new EvictedCacheEntry(iter.getCacheableData().getBuffer(), iter.getCacheableData(), resourceId);
				return tmp;
			}
		}
		
		
		//remove entry in T2
		
			DoubleLinkedList iter = T2_tail;
			while (iter != null && iter.isPinned())
				iter = iter.getPrev();
			if (iter != null) {
				if (iter != T2_head) {
					iter.getPrev().addNext(iter.getNext());
					if (iter != T2_tail)
						iter.getNext().addPrev(iter.getPrev());
					else
						T2_tail = iter.getPrev();
				}
				else {
					T2_head = iter.getNext();
					T2_head.addPrev(null);
				}
				T2_length--;
				B2_length++;
				DoubleLinkedList tt = new DoubleLinkedList(iter.getCacheableData().getPageNumber(), resourceId);
				if (B2_head != null) {
					B2_head.addPrev(tt);
					tt.addNext(B2_head);
					B2_head = tt;
				}
				else {
					B2_head = B2_tail = tt;
				}
				EvictedCacheEntry tmp = new EvictedCacheEntry(iter.getCacheableData().getBuffer(), iter.getCacheableData(), resourceId);
				return tmp;
			}
			else
				throw new CachePinnedException();
		
	}
	
	
	@Override
	public EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
	
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId)
				throw new DuplicateCacheEntryException(newPage.getPageNumber(), resourceId);
			tmp = tmp.getNext();
		}
		
		tmp = T2_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId)
				throw new DuplicateCacheEntryException(newPage.getPageNumber(), resourceId);
			tmp = tmp.getNext();
		}
		
		//Case II: x is in B1.
	    tmp = B1_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId) {
				p = p + (B1_length > B2_length ? 1: (B2_length / B1_length));
				p = (p > c ? c : p);
				DoubleLinkedList tt = new DoubleLinkedList(newPage, resourceId, false);
				if (T2_head != null) {
					T2_head.addPrev(tt);
					tt.addNext(T2_head);
					T2_head = tt;
				}
				else {
					T2_head = T2_tail = tt;
				}
				T2_length++;
				return subroutine_replace(newPage, resourceId, false);
			}
			tmp = tmp.getNext();
		}
		
		//Case III: x is in B2
		tmp = B2_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId) {
				p = p - (B2_length > B1_length ? 1: (B1_length / B2_length));
				p = (p > 0 ? p : 0);
				DoubleLinkedList tt = new DoubleLinkedList(newPage, resourceId, false);
				if (T2_head != null) {
					T2_head.addPrev(tt);
					tt.addNext(T2_head);
					T2_head = tt;
				}
				else {
					T2_head = T2_tail = tt;
				}
				T2_length++;
				return subroutine_replace(newPage, resourceId, true);
			}
			tmp = tmp.getNext();
		}
		
		//Case IV: x is totally missing
		EvictedCacheEntry ece = null;
		if (T1_length + B1_length == c) {
			if (T1_length < c) {
				if (B1_length != 1) {
					B1_tail.getPrev().addNext(null);
					B1_tail = B1_tail.getPrev();
				}
				else {
					B1_tail = B1_head = null;
				}
				B1_length--;
				subroutine_replace(newPage, resourceId, false);	
			} 
			else {
				DoubleLinkedList iter = T1_tail;
				while (iter != null && iter.isPinned())
					iter = iter.getPrev();
				if (iter != null) {
					if (iter != T1_head) {
						iter.getPrev().addNext(iter.getNext());
						if (iter != T1_tail)
							iter.getNext().addPrev(iter.getPrev());
						else
							T1_tail = iter.getPrev();
					}
					else {
						T1_head = iter.getNext();
						T1_head.addPrev(null);
					}
					T1_length--;
					ece = new EvictedCacheEntry(iter.getCacheableData().getBuffer(), iter.getCacheableData(), resourceId);
				}
			}
		}
		else {
			if (T1_length + T2_length + B1_length + B2_length >= c) {
				if (T1_length + T2_length + B1_length + B2_length == 2 * c) {
					if (B2_length != 1) {
						B2_tail.getPrev().addNext(null);
						B2_tail = B2_tail.getPrev();
					}
					else {
						B2_tail = B2_head = null;
					}
					B2_length--;
				}
				ece = subroutine_replace(newPage, resourceId, false);
			}
		}
		
		DoubleLinkedList t = new DoubleLinkedList(newPage, resourceId, false);
		if (T1_length != 0) {
			T1_head.addPrev(t);
			t.addNext(T1_head);
			T1_head = t;
		}
		else
			T1_head = T1_tail = t;
		T1_length++;
		if (ece == null) {
			byte [] x = new byte[this.pageSize];
			ece = new EvictedCacheEntry(x,null,-1);
		}
		return ece;
	}

	@Override
	public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
		
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId)
				throw new DuplicateCacheEntryException(newPage.getPageNumber(), resourceId);
			tmp = tmp.getNext();
		}
		
		tmp = T2_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId)
				throw new DuplicateCacheEntryException(newPage.getPageNumber(), resourceId);
			tmp = tmp.getNext();
		}
		
		//Case II: x is in B1.
	    tmp = B1_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId) {
				p = p + (B1_length > B2_length ? 1: (B2_length / B1_length));
				p = (p > c ? c : p);
				DoubleLinkedList tt = new DoubleLinkedList(newPage, resourceId, false);
				if (T2_head != null) {
					T2_head.addPrev(tt);
					tt.addNext(T2_head);
					T2_head = tt;
				}
				else {
					T2_head = T2_tail = tt;
				}
				T2_length++;
				return subroutine_replace(newPage, resourceId, false);
			}
			tmp = tmp.getNext();
		}
		
		//Case III: x is in B2
		tmp = B2_head;
		while (tmp != null) {
			if (tmp.getPageNumber() == newPage.getPageNumber() && tmp.getResourceId() == resourceId) {
				p = p - (B2_length > B1_length ? 1: (B1_length / B2_length));
				p = (p > 0 ? p : 0);
				DoubleLinkedList tt = new DoubleLinkedList(newPage, resourceId, false);
				if (T2_head != null) {
					T2_head.addPrev(tt);
					tt.addNext(T2_head);
					T2_head = tt;
				}
				else {
					T2_head = T2_tail = tt;
				}
				T2_length++;
				return subroutine_replace(newPage, resourceId, true);
			}
			tmp = tmp.getNext();
		}
		
		//Case IV: x is totally missing
		EvictedCacheEntry ece = null;
		if (T1_length + B1_length == c) {
			if (T1_length < c) {
				if (B1_length != 1) {
					B1_tail.getPrev().addNext(null);
					B1_tail = B1_tail.getPrev();
				}
				else {
					B1_tail = B1_head = null;
				}
				B1_length--;
				subroutine_replace(newPage, resourceId, false);	
			} 
			else {
				DoubleLinkedList iter = T1_tail;
				while (iter != null && iter.isPinned())
					iter = iter.getPrev();
				if (iter != null) {
					if (iter != T1_head) {
						iter.getPrev().addNext(iter.getNext());
						if (iter != T1_tail)
							iter.getNext().addPrev(iter.getPrev());
						else
							T1_tail = iter.getPrev();
					}
					else {
						T1_head = iter.getNext();
						T1_head.addPrev(null);
					}
					T1_length--;
					ece = new EvictedCacheEntry(iter.getCacheableData().getBuffer(), iter.getCacheableData(), resourceId);
				}
			}
		}
		else {
			if (T1_length + T2_length + B1_length + B2_length >= c) {
				if (T1_length + T2_length + B1_length + B2_length == 2 * c) {
					if (B2_length != 1) {
						B2_tail.getPrev().addNext(null);
						B2_tail = B2_tail.getPrev();
					}
					else {
						B2_tail = B2_head = null;
					}
					B2_length--;
				}
				ece = subroutine_replace(newPage, resourceId, false);
			}
		}
		
		DoubleLinkedList t = new DoubleLinkedList(newPage, resourceId, true);
		if (T1_length != 0) {
			T1_head.addPrev(t);
			t.addNext(T1_head);
			T1_head = t;
		}
		else
			T1_head = T1_tail = t;
		T1_length++;
		if (ece == null) {
			byte [] x = new byte[this.pageSize];
			ece = new EvictedCacheEntry(x,null,-1);
		}
		return ece;
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getCacheableData().getPageNumber() == pageNumber && tmp.getResourceId() == resourceId)
				tmp.unPinn();
			tmp = tmp.getNext();
		}
		
		tmp = T2_head;
		while (tmp != null) {
			if (tmp.getCacheableData().getPageNumber() == pageNumber && tmp.getResourceId() == resourceId)
				tmp.unPinn();
			tmp = tmp.getNext();
		}
		
	}

	@Override
	public CacheableData[] getAllPagesForResource(int resourceId) {
		CacheableData[] cad;
		cad = new CacheableData[c];
		int length = 0;
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId) 
				cad[length++] = this.getPage(resourceId, tmp.getPageNumber());
			tmp = tmp.getNext();
		}
		tmp = T2_head;
		while (tmp != null) {
			if (tmp.getResourceId() == resourceId) 
				cad[length++] = this.getPage(resourceId, tmp.getPageNumber());
			tmp = tmp.getNext();
		}
		
		return cad;
	}

	@Override
	public void expellAllPagesForResource(int resourceId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getCapacity() {
		return this.c;
	}

	@Override
	public void unpinAllPages() {
		DoubleLinkedList tmp = T1_head;
		while (tmp != null) {
				tmp.unPinn();
			tmp = tmp.getNext();
		}
		
		tmp = T2_head;
		while (tmp != null) {
				tmp.unPinn();
			tmp = tmp.getNext();
		}
		
	}

}
