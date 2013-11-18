package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;

/**
 * freebuffer contains readbuffer and writebuffer
 * when buffer is full, which means no buffer for read, need to flush the writebuffer
 * @author qml_moon
 *
 */
public class FreeBuffer {
	private byte [][] buffer;
	private EvictedCacheEntry [] wrapper;
	
	private ResourceManager[] resourceManager;
	
	private int pageSize;
	
	private volatile int writeNum;
	
	private int maxNum;
	
	private Rqueue rthread;
	private WriteThread wthread;
	
	
	public FreeBuffer(int page_size, int max_num, Rqueue read_thread, WriteThread write_thread) {
		buffer = new byte[max_num][];
		for (int i = 0; i < max_num; i++) 
			buffer[i] = new byte[page_size];
		wrapper = new EvictedCacheEntry[max_num];
		resourceManager = new ResourceManager[max_num];
		pageSize = page_size;
		maxNum = max_num;
		writeNum = 0;
		rthread = read_thread;
		wthread = write_thread;
	}
	
	/**
	 * if the evicted has not been modified, ignore it.
	 * @param entry
	 * @param resource_manager
	 */
	public void addWriteEntry(EvictedCacheEntry entry, ResourceManager resource_manager) {
		if (entry.getWrappingPage().hasBeenModified() && (writeNum < maxNum)) {
			wrapper[writeNum] = entry;
			resourceManager[writeNum] = resource_manager;
//			buffer[writeNum] = entry.getBuffer();
			writeNum++;
		
			
//	TODO should not be like this.......
			synchronized(buffer) {
				wthread.wakeup(buffer, resourceManager, wrapper, writeNum);
				try {
					buffer.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//			Request r = new Request(resource_manager, entry);
//			while (!this.wthread.enQueue(r));
				writeNum = 0;
			}
			
		}
	}
	
	public int getWriteNum() {
		return writeNum;
	}
	
	public void setWriteNum(int x) {
		this.writeNum = x;
	}
	
	public byte[] getReadBuffer() throws InterruptedException {
		if (writeNum >= maxNum) {
//			synchronized(buffer) {
//				wthread.wakeup(buffer, resourceManager, wrapper, maxNum);
//				buffer.wait();
//			}
//			writeNum = 0;
		}
		buffer[writeNum] = new byte[this.pageSize];
		return buffer[writeNum];
	}
	
	public byte[] getWriteBuffer(int num) {
		//return buffer[num];
		return wrapper[num].getBinaryPage();
	}
	
	public EvictedCacheEntry getData(int num) {
		return wrapper[num];
	}
	
	/**
	 * check whether the requested page is in the write buffer and not in the cache.
	 * @param resourceId
	 * @param pageNum
	 * @return
	 */
	public CacheableData findPage(int resourceId, int pageNum) {
		for (int i = 0; i < writeNum; i ++)
			if (wrapper[i].getResourceID() == resourceId && wrapper[i].getPageNumber() == pageNum)
				return wrapper[i].getWrappingPage();
		return null;
	}
}
