package de.tuberlin.dima.minidb.io.cache;

import java.util.Iterator;
import java.util.LinkedList;

import de.tuberlin.dima.minidb.util.Pair;

public class PageCacheImpl implements PageCache {

	private PageSize pageSize;
	private int numPages;
	
	byte[][] buffers;
	int bufferCount = 0;
	
	private int p;
	private static final int DEFAULT_P = 0;
	
	private LinkedList<CacheEntry> t1,t2;
	private LinkedList<Pair<Integer,Integer>> b1,b2;
	
	int expellCountT1 = 0, expellCountT2 = 0;
	
	public PageCacheImpl(PageSize pageSize, int numPages) {
		this.pageSize = pageSize;
		this.numPages = numPages;
		this.p = DEFAULT_P;//numPages/2;

		this.buffers = new byte[numPages][pageSize.getNumberOfBytes()];
		
		this.t1 = new LinkedList<CacheEntry>();
		this.t2 = new LinkedList<CacheEntry>();
		
		//fill T1 and T2 with empty page entries
		/*int i;
		for(i=0; i<p; ++i) {
			t1.add(new CacheEntry(buffers[i], null, -1));
		}
		for(; i< numPages; ++i) {
			t2.add(new CacheEntry(buffers[i], null, -1));
		}*/
		
		this.b1 = new LinkedList<Pair<Integer,Integer>>();
		this.b2 = new LinkedList<Pair<Integer,Integer>>();
	}
	
	@Override
	public CacheableData getPage(int resourceId, int pageNumber) {
		
		for(CacheEntry itr : t2) {
			if(!itr.isExpelled() && itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				itr.increaseHit();
				t2.remove(itr);
				t2.addFirst(itr);
				return itr.getWrappingPage();
			}
		}
		
		for(CacheEntry itr : t1) {
			if(!itr.isExpelled() && itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				if(itr.increaseHit() > 1) {
					t1.remove(itr);
					t2.addFirst(itr);
				}
				return itr.getWrappingPage();
			}
		}
		
		//TODO: update p here or not ???
/*		for(Pair<Integer,Integer> itr : b1) {
			if(itr.getFirst() == resourceId && itr.getSecond() == pageNumber) {
				int sigma = b1.size() >= b2.size() ? 1 : b2.size()/b1.size();
				p = p+sigma > numPages ? numPages : p+sigma;
				return null;
			}
		}
		
		for(Pair<Integer,Integer> itr : b2) {
			if(itr.getFirst() == resourceId && itr.getSecond() == pageNumber) {
				int sigma = b2.size() >= b1.size() ? 1 : b1.size()/b2.size();
				p -= sigma;
				if(p<0) p=0;
				return null;
			}
		}
		*/
		return null;
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber) {
		
		for(CacheEntry itr : t2) {
			if(!itr.isExpelled() && itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				itr.increaseHit();
				t2.remove(itr);
				t2.addFirst(itr);
				itr.increasePinned(); 
				return itr.getWrappingPage();
			}
		}
		
		for(CacheEntry itr : t1) {
			if(!itr.isExpelled() && itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				if(itr.increaseHit() > 1) {
					t1.remove(itr);
					t2.addFirst(itr);
				}
				itr.increasePinned(); 
				return itr.getWrappingPage();
			}
		}
		
		//TODO: update p here or not ???
/*		for(Pair<Integer,Integer> itr : b1) {
			if(itr.getFirst() == resourceId && itr.getSecond() == pageNumber) {
				int sigma = b1.size() >= b2.size() ? 1 : b2.size()/b1.size();
				p = p+sigma > numPages ? numPages : p+sigma;
				return null;
			}
		}
		
		for(Pair<Integer,Integer> itr : b2) {
			if(itr.getFirst() == resourceId && itr.getSecond() == pageNumber) {
				int sigma = b2.size() >= b1.size() ? 1 : b1.size()/b2.size();
				p -= sigma;
				if(p<0) p=0;
				return null;
			}
		}
		*/
		return null;

	}
	
	private EvictedCacheEntry evictFromList(int listNumber) throws CachePinnedException {
		CacheEntry evict = null;
		Iterator<CacheEntry> itr = null;
		LinkedList<CacheEntry> l1=null, l2=null;
		
		//remove expelled entry first
		if(expellCountT1 > 0) {
			itr = t1.descendingIterator();
			while(itr.hasNext()) {
				evict = itr.next();
				if(evict.isExpelled()) {
					itr.remove();
					expellCountT1--;
					return evict;
				}
			}
			
		}
		else if(expellCountT2 > 0) {
			itr = t2.descendingIterator();
			while(itr.hasNext()) {
				evict = itr.next();
				if(evict.isExpelled()) {
					itr.remove();
					expellCountT2--;
					return evict;
				}
			}
		}
		
		if(listNumber == 1) {	
			l1 = t1;
			l2 = t2;
		}
		else if(listNumber == 2) {
			l1 = t2;
			l2 = t1;
		}
		
		if(!l1.isEmpty()) {
			if(l1.peekLast().isExpelled()) {
				return l1.removeLast();
			}
			//check unpinned page
			itr = l1.descendingIterator();
			while(itr.hasNext()) {
				evict = itr.next();
				if(!evict.isPinned()) {
					itr.remove();
					return evict;
				}
			}
		} 
		else {
			return null;
		}
		
		//if T1 is empty or no more unpinned page exists, then evict a page in T2
		if(!l2.isEmpty()) {
			//remove expelled entry first
			if(l2.peekLast().isExpelled()) {
				return l2.removeLast();
			}
			itr = l2.descendingIterator();
			while(itr.hasNext()) {
				evict = itr.next();
				if(!evict.isPinned()) {
					itr.remove();
					return evict;
				}
			}
		}
		
		throw new CachePinnedException();
	}
	
	private EvictedCacheEntry subroutine_replace(boolean in_B2) 
			throws CachePinnedException,DuplicateCacheEntryException {

		EvictedCacheEntry evict = null;

		// subroutine replace
		if (!t1.isEmpty() && ((t1.size() > p) || (t1.size() == p && in_B2))) {
			// delete the LRU page in T1 and add it to MRU of B1
			evict = evictFromList(1);// t1.removeLast();
			b1.addFirst(new Pair<Integer, Integer>(evict.getResourceID(), evict
					.getPageNumber()));
		} else {
			// delete the LRU page in T2 and add it to MRU of B2
			evict = evictFromList(2);// t2.removeLast();
			b2.addFirst(new Pair<Integer, Integer>(evict.getResourceID(), evict
					.getPageNumber()));
		}
		return evict;
	}
	
	@Override
	public EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
		EvictedCacheEntry evict = null;
		
		int pageNumber = newPage.getPageNumber();
		for(CacheEntry itr : t1) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				throw new DuplicateCacheEntryException(pageNumber, resourceId);
			}
		}
		for(CacheEntry itr : t2) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				throw new DuplicateCacheEntryException(pageNumber, resourceId);
			}
		}
		
		Pair<Integer, Integer> pair = new Pair<Integer,Integer>(resourceId, pageNumber);
		
		if(b2.contains(pair)) {
			int sigma = (b2.size() >= b1.size() ? 1 : b1.size()/b2.size());
			p -= sigma;
			if(p<0) p=0;
			

			evict = this.subroutine_replace(true);
			b2.remove(pair);
			t2.addFirst(new CacheEntry(newPage, resourceId, false));
			return evict;
		}
		
		if(b1.contains(pair)) {
			int sigma = (b1.size() >= b2.size() ? 1 : b2.size()/b1.size());
			p = (p+sigma > numPages ? numPages : p+sigma);
			
			evict = this.subroutine_replace(false);
			b1.remove(pair);
			t2.addFirst(new CacheEntry(newPage, resourceId, false));
			return evict;
		}

		
		if(t1.size() + b1.size() == numPages) {
			if(t1.size() < numPages) {
				b1.removeLast();
				evict = this.subroutine_replace(false);
			}
			else {
				evict = evictFromList(1);
			}
		}
		else {
			int total = t1.size() + t2.size() + b1.size() + b2.size();
			if(total >= numPages) {
				if(total == 2*numPages)
					b2.removeLast();
				evict = this.subroutine_replace(false);
			}
		}
		
		t1.addFirst(new CacheEntry(newPage, resourceId, false));
		if(evict == null && bufferCount < numPages) {
			evict = new EvictedCacheEntry(buffers[bufferCount], null, -1);
			bufferCount++;
		}
		return evict;
	}

	@Override
	public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
		EvictedCacheEntry evict = null;
		
		int pageNumber = newPage.getPageNumber();
		for(CacheEntry itr : t1) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				throw new DuplicateCacheEntryException(pageNumber, resourceId);
			}
		}
		for(CacheEntry itr : t2) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				throw new DuplicateCacheEntryException(pageNumber, resourceId);
			}
		}
		
		Pair<Integer, Integer> pair = new Pair<Integer,Integer>(resourceId, pageNumber);
		
		if(b2.contains(pair)) {

			int sigma = b2.size() >= b1.size() ? 1 : b1.size()/b2.size();
			p -= sigma;
			if(p<0) p=0;
			

			evict = this.subroutine_replace(true);
			b2.remove(pair);
			t2.addFirst(new CacheEntry(newPage, resourceId, true, true));
			return evict;
		}
		
		if(b1.contains(pair)) {
			int sigma = b1.size() >= b2.size() ? 1 : b2.size()/b1.size();
			p = p+sigma > numPages ? numPages : p+sigma;
			
			evict = this.subroutine_replace(false);
			b1.remove(pair);
			t2.addFirst(new CacheEntry(newPage, resourceId, true, true));
			return evict;
		}

		
		if(t1.size() + b1.size() == numPages) {
			if(t1.size() < numPages) {
				b1.removeLast();
				evict = this.subroutine_replace(false);
			}
			else {
				evict = evictFromList(1);
			}
		}
		else {
			int total = t1.size() + t2.size() + b1.size() + b2.size();
			if(total >= numPages) {
				if(total == 2*numPages)
					b2.removeLast();
				evict = this.subroutine_replace(false);
			}
		}
		
		
		t1.addFirst(new CacheEntry(newPage, resourceId, true, true));
		return evict;
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		
		for(CacheEntry itr : t1) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				itr.decreasePinned();
				return;
			}
		}
		
		for(CacheEntry itr : t2) {
			if(itr.getResourceID() == resourceId && itr.getPageNumber() == pageNumber) {
				itr.decreasePinned();
				return;
			}
		}
	}

	@Override
	public CacheableData[] getAllPagesForResource(int resourceId) {
		
		LinkedList<Integer> pageNumbers = new LinkedList<Integer>();
		
		for(CacheEntry itr : t1) {
			if(itr.getResourceID() == resourceId) {
				pageNumbers.add(itr.getPageNumber());
			}
		}
		
		for(CacheEntry itr : t2) {
			if(itr.getResourceID() == resourceId) {
				pageNumbers.add(itr.getPageNumber());
			}
		}
		
		CacheableData[] result = new CacheableData[pageNumbers.size()];
		
		int i=0;
		for(Integer num : pageNumbers) {
			result[i] = this.getPage(resourceId, num);
			i++;
		}
		
		return result;
	}

	
	@Override
	public void expellAllPagesForResource(int resourceId) {
		
		int i, n;
		for(i=0, n=0; n<t1.size(); ++n) {
			CacheEntry cur = t1.get(i);
			if(cur.getResourceID() == resourceId) {
				cur.markExpelled();
				//move the expelled entry to the end of the list
				CacheEntry removed = t1.remove(i);
				t1.addLast(removed);
				expellCountT1++;
			}
			else {
				i++;
			}
		}

		for(i=0, n=0; n<t2.size(); ++n) {
			CacheEntry cur = t2.get(i);
			if(cur.getResourceID() == resourceId) {
				cur.markExpelled();
				//move the expelled entry to the end of the list
				CacheEntry removed = t2.remove(i);
				t2.addLast(removed);
				expellCountT2++;
			}
			else {
				i++;
			}
		}
	}

	@Override
	public int getCapacity() {
		return this.numPages;
	}

	@Override
	public void unpinAllPages() {
		
		for(CacheEntry itr : t1) {
			itr.setUnpinned();
		}
		for(CacheEntry itr : t2) {
			itr.setUnpinned();
		}	
	}

}