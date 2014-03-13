package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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

//		System.out.println("start to create record reader..");


		if(getDBInstance() == null){
			throw new RuntimeException("TableInputFormat is not initialized!");
		}

		TableRecordReader reader = new TableRecordReader(getDBInstance());

//		reader.initialize(split, context);

		return reader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
//		System.out.println("start to get splits..");

		LinkedList<InputSplit> splitList = new LinkedList<InputSplit>();

		if(getDBInstance() == null){
			throw new RuntimeException("TableInputFormat is not initialized!");
		}

		if(context.getConfiguration().get("dbms.input_table_name") == null){
			throw new RuntimeException("input table names are null");
		}

		String tablenames[] = context.getConfiguration().get("dbms.input_table_name").split(",");

//		System.out.println("num of tablenames: " + tablenames.length);

		int SPLITS_PER_TABLE = context.getConfiguration().getInt("dbms.input_splits_per_table", 0);

		int PAGES_PER_SPLIT = context.getConfiguration().getInt("dbms.pages_per_input_split", 0);

		if(SPLITS_PER_TABLE != 0 && PAGES_PER_SPLIT !=0){
			throw new RuntimeException("only one conf of SPLITS_PER_TABLE and PAGES_PER_SPLIT can be set!");
		}

		for (int i = 0; i < tablenames.length; i++){

//			System.out.println("table: " + tablenames[i]);

			TableResourceManager rm = getDBInstance().getCatalogue().getTable(tablenames[i]).getResourceManager();
			if(rm == null ){
				throw new RuntimeException("Table: " + tablenames[i]  +" cannot be opened!");
			}

			int numPages = (rm.getLastDataPageNumber()+1 - rm.getFirstDataPageNumber()  );

//			int tableSize = numPages * rm.getPageSize().getNumberOfBytes();

			if(SPLITS_PER_TABLE > 0 ){

				System.out.println("TABLE: " + tablenames[i] + " SPLITS_PER_TABLE: " + SPLITS_PER_TABLE);
				System.out.println("numPages: " + numPages);
				int currentPage = rm.getFirstDataPageNumber();

				if(numPages <= SPLITS_PER_TABLE){
					for(int n = 0; n < numPages; n++){
						splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+1, rm.getPageSize().getNumberOfBytes()));
						currentPage ++;
					}
//					for(int n = numPages; n < SPLITS_PER_TABLE; n++){
//						splitList.add(new TableInputSplit(tablenames[i], 0, 0, 0));
//					}
				}else{
					int avg =  numPages / SPLITS_PER_TABLE;
					int radius = numPages % SPLITS_PER_TABLE ;

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

				}
				if(currentPage != rm.getLastDataPageNumber() + 1){
					throw new RuntimeException("Internal Error: some pages are ommitted..");
				}

			}
			else if(PAGES_PER_SPLIT > 0){

//				System.out.println("PAGES_PER_SPLIT: " + PAGES_PER_SPLIT);


				int numSplits = numPages / PAGES_PER_SPLIT;
				int radius = numPages % PAGES_PER_SPLIT;

				int currentPage = rm.getFirstDataPageNumber();
				for(int s = 0; s < numSplits; s ++){					
					splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+PAGES_PER_SPLIT, rm.getPageSize().getNumberOfBytes()));
					currentPage += PAGES_PER_SPLIT;
				}

				if(radius != 0 ){
					numSplits ++;
					splitList.add(new TableInputSplit(tablenames[i], currentPage, currentPage+radius, rm.getPageSize().getNumberOfBytes()));
					currentPage += radius;
				}

				if(currentPage != rm.getLastDataPageNumber() +1){
					throw new RuntimeException("Internal Error: some pages are ommitted..");
				}
			}

//			rm.closeResource();

		}


		System.out.println("get splits finished.." + "Size: "  + splitList.size());
		return splitList;
	}

}