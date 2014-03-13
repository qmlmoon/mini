package de.tuberlin.dima.minidb.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class TableInputSplit extends InputSplit implements Writable{

	private String tableName;
	private int firstPage;
	private int lastPage; //excluded
	private int pageSize;

	public TableInputSplit() {
		this.firstPage = -1;
		this.lastPage = -1;
		this.pageSize = -1;
	}

	public TableInputSplit(String tablename, int firstpage, int lastpage, int pagesize){
		this.tableName = tablename;
		this.firstPage = firstpage;
		this.lastPage = lastpage;
		this.pageSize = pagesize;
	}



	@Override
	public void readFields(DataInput input) throws IOException {
		this.tableName = input.readUTF();
		this.firstPage = input.readInt();
		this.lastPage = input.readInt();
		this.pageSize = input.readInt();
//		System.out.println("TableInputSplit read input..");
	}

	@Override
	public void write(DataOutput output) throws IOException {

		output.writeUTF(tableName);
		output.writeInt(firstPage);
		output.writeInt(lastPage);
		output.writeInt(pageSize);

//		System.out.println("TableInputSplit write output..");
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
//		System.out.println("TableInputSplit get length..");
		return ((lastPage - firstPage) * pageSize);
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		//TODO
//		System.out.println("TableInputSplit get location..");
		return new String[0];
	}

	public String getTable(){
		return this.tableName;
	}

	public int getFirstPage(){
		return this.firstPage;
	}

	public int getLastPage(){
		return this.lastPage;
	}

	public int getPageSize(){
		return this.pageSize;
	}
}