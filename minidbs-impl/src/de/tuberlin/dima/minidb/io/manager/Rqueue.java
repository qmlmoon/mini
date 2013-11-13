package de.tuberlin.dima.minidb.io.manager;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Request Queue, which store object Request
 * 
 * TODO improve the performance by designing a queuing schema
 * @author Titicaca
 *
 */
public class Rqueue{
	private Queue<Request> rqueue;
	private int maxSize;
	
	public Rqueue(){
		maxSize = BufferPoolManager.MAX_PAGE_REQUESTS_IN_SINGLE_QUEUE;
		rqueue = new LinkedList<Request>();
	}
	
	public Rqueue(int queueSize){
		maxSize = queueSize;
		rqueue = new LinkedList<Request>();
	}
	
	/*
	 * return false if queue is already full
	 */
	public boolean enQueue(Request r){
		if(rqueue.size() < maxSize){
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
	
}