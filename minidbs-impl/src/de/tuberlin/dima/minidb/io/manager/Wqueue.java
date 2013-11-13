package de.tuberlin.dima.minidb.io.manager;

import java.util.LinkedList;
import java.util.Queue;

import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;

/**
 * Writing Queue, which stores object EvictedCacheEntry
 * @author Titicaca
 *
 */
public class Wqueue{
	private Queue<EvictedCacheEntry> wqueue;
	private int maxSize;
	
	public Wqueue(){
		//TODO default size, is the same as request queue?
		maxSize = BufferPoolManager.MAX_PAGE_REQUESTS_IN_SINGLE_QUEUE;
		wqueue = new LinkedList<EvictedCacheEntry>();
	}
	
	public Wqueue(int queueSize){
		maxSize = queueSize;
		wqueue = new LinkedList<EvictedCacheEntry>();
	}
	
	/*
	 * return false if queue is already full
	 */
	public boolean enQueue(EvictedCacheEntry r){
		if(wqueue.size() < maxSize){
			return wqueue.offer(r);
		}
		else{
			return false;
		}
	}
	
	/*
	 * return null if queue is empty
	 */
	public EvictedCacheEntry deQueue(){
		if(wqueue.size() > 0){
			return wqueue.poll();
		}else{
			return null;
		}
	}
}