package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Read request Queue, which store object Request
 * 
 * TODO improve the performance by designing a queuing schema
 * @author Titicaca
 *
 */
public class Rqueue extends Thread {
	private volatile Queue<Request> rqueue;
	private int maxSize;
	private volatile boolean alive;
	
	public Rqueue(){
		maxSize = BufferPoolManager.MAX_PAGE_REQUESTS_IN_SINGLE_QUEUE;
		rqueue = new LinkedList<Request>();
		this.alive = true;
	}
	
	public Rqueue(int queueSize){
		maxSize = queueSize;
		rqueue = new LinkedList<Request>();
		this.alive = true;
	}
	
	/*
	 * return false if queue is already full
	 */
	public synchronized boolean enQueue(Request r){
		if(rqueue.size() < maxSize){
			System.out.println(r.getPageNumber());
			for (int i = 0; i < rqueue.size(); i++) {
				Request tmp = rqueue.poll();
				rqueue.offer(tmp);
				if (tmp.getResourceId() == r.getResourceId() && tmp.getPageNumber() == r.getPageNumber())
					return false;
			}
			return rqueue.offer(r);
		}
		else{
			return false;
		}
	}
	
	/*
	 * return null if queue is empty
	 */
	public Request deQueue(){
		if(rqueue.size() > 0){
			return rqueue.poll();
		}else{
			return null;
		}
	}
	
	public void shutdown() {
		this.alive = false;
	}
	
	public void run() {
		while (alive) {
			while (alive && rqueue.size() == 0);
			if (!alive)
				break;
			Request re = this.deQueue();
			synchronized(re) {

			CacheableData newPage = null;
			try {
				newPage = re.getFreeBuffer().findPage(re.getResourceId(), re.getPageNumber());
				if (newPage == null) {
					newPage = re.getResourceManager().readPageFromResource(re.getFreeBuffer().getReadBuffer(), re.getPageNumber());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				re.setNewPage(newPage);
				re.notifyAll();
			}
		}
	}
	
}