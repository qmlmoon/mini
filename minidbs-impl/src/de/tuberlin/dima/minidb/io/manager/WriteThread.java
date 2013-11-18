package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;

public class WriteThread extends Thread {
	private byte [][] buffer;
	
	private volatile boolean active;
	
	private ResourceManager[] resourceManager;
	
	private EvictedCacheEntry[] wrapper;
	
	private int maxNum;
	
	private volatile boolean alive;
	
	private volatile Queue<Request> wqueue;

	private int maxSize;
	public WriteThread(int max_num) {
		this.maxNum = max_num;
		this.active = false;
		this.alive = true;
		wqueue = new LinkedList<Request>();
		maxSize = max_num;
	}
	
	public void shutdown() {
		this.alive = false;
	}
	
	public boolean isActive(){
		return this.active;
	}
//	
//	public synchronized boolean enQueue(Request r) {
//		if(wqueue.size() < maxSize){
//			return wqueue.offer(r);
//		}
//		else{
//			return false;
//		}
//	}
//
//	public Request deQueue(){
//		if(wqueue.size() > 0){
//			return wqueue.poll();
//		}else
//			return null;
//	}
//
//	
//	public void run() {
//		while (alive) {
//			while (alive && wqueue.size() == 0);
//			if (!alive)
//				break;
//			
//			Request r = this.deQueue();
//			System.out.println("write" + wqueue.size());
//			try {
//				if (r.getEvicted() == null)
//					System.out.println("ev");
//				if (r.getResourceManager() == null)
//					System.out.println("rm");
//				r.getResourceManager().writePageToResource(r.getEvicted().getBinaryPage(), r.getEvicted().getWrappingPage());
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
	
	public synchronized void wakeup(byte [][] buff, ResourceManager[] rm, EvictedCacheEntry[] ev, int num) {
		buffer = buff;
		resourceManager = rm;
		wrapper = ev;
		maxNum = num;
		this.active = true;
	}
	

	
	
	public void run() {
		while (alive) {
			while (alive && !this.active);
			if (!alive)
				break;
			synchronized(buffer) {
				
			for (int i = 0; i < maxNum; i++)
				try {				
					resourceManager[i].writePageToResource(wrapper[i].getBinaryPage(), wrapper[i].getWrappingPage());
				}
				catch (IOException e) {
					e.printStackTrace();
					}
				buffer.notify();
				this.active = false;
			}
		}
	}
}
