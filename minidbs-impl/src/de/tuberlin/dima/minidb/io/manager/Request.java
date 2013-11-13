package de.tuberlin.dima.minidb.io.manager;

public class Request{
	private int resourceId;
	private int pageNumber;
	
	public Request(int resource_id, int page_number){
		resourceId = resource_id;
		pageNumber = page_number;
	}
	
	public int getResourceId(){
		return this.resourceId;
	}
	
	public int getPageNumber(){
		return this.pageNumber;
	}
}