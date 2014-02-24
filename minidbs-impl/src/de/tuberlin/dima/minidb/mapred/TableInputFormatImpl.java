package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;


public class TableInputFormatImpl extends TableInputFormat{

	
	@Override
	public RecordReader<Text, DataTuple> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		if(getDBInstance() == null){
			throw new RuntimeException("TableInputFormat is not initialized!");
		}
		
		TableRecordReader reader = new TableRecordReader(getDBInstance());
		
		//reader.initialize(split, context);
		
		return reader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		
		
		ArrayList<InputSplit> splitList = new ArrayList<InputSplit>();
		
		if(getDBInstance() == null){
			throw new RuntimeException("TableInputFormat is not initialized!");
		}
		
		if(context.getConfiguration().get("dbms.input_table_name") == null){
			throw new RuntimeException("input table names are null");
		}
		
		String tablenames[] = context.getConfiguration().get("dbms.input_table_name").split(",");
		
		int SPLITS_PER_TABLE = context.getConfiguration().getInt("dbms.input_splits_per_table", 0);
		
		int PAGES_PER_SPLIT = context.getConfiguration().getInt("dbms.pages_per_input_split", 0);
		
		if(SPLITS_PER_TABLE != 0 && PAGES_PER_SPLIT !=0){
			throw new RuntimeException("only one conf of SPLITS_PER_TABLE and PAGES_PER_SPLIT can be set!");
		}
		
		for (int i = 0; i < tablenames.length; i++){
			TableResourceManager rm = getDBInstance().getCatalogue().getTable(tablenames[i]).getResourceManager();
			if(rm == null ){
				throw new RuntimeException("Table: " + tablenames[i]  +" cannot be opened!");
			}
			
			int numPages = (rm.getFirstDataPageNumber() - rm.getLastDataPageNumber() );
			int tableSize = numPages * rm.getPageSize().getNumberOfBytes();
			
			if(SPLITS_PER_TABLE > 0 ){
				if(numPages <= SPLITS_PER_TABLE){
					for(int n = 0; n < numPages; n++){
						splitList.add(new TableInputSplit(tablenames[i], n, n+1, rm.getPageSize().getNumberOfBytes()));
					}
				}else{
					int avg = SPLITS_PER_TABLE / numPages;
					int radius = SPLITS_PER_TABLE % numPages;
					int currentPage = 0;
					
					for(int n = 0; n < SPLITS_PER_TABLE; n ++){
						if(radius != 0 ){
							splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+avg+1, rm.getPageSize().getNumberOfBytes()));
							currentPage += (avg +1);
							radius --;
						}else{
							splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+avg, rm.getPageSize().getNumberOfBytes()));
							currentPage += avg;
						}
					}
					
					if(currentPage != numPages){
						throw new RuntimeException("Internal Error: some pages are ommitted..");
					}
					
				}
			}
			else if(PAGES_PER_SPLIT > 0){
				int numSplits = numPages / PAGES_PER_SPLIT;
				int radius = numPages % PAGES_PER_SPLIT;

				int currentPage = 0;
				for(int s = 0; s < numSplits; s ++){					
					splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+PAGES_PER_SPLIT, rm.getPageSize().getNumberOfBytes()));
					currentPage += PAGES_PER_SPLIT;
				}
				
				if(radius != 0 ){
					numSplits ++;
					splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+radius, rm.getPageSize().getNumberOfBytes()));
					currentPage += radius;
				}
				
				if(currentPage != numPages){
					throw new RuntimeException("Internal Error: some pages are ommitted..");
				}
			}
	
		}
		
		
		
		return splitList;
	}
	
}