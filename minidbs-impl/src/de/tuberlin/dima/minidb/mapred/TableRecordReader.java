package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

public class TableRecordReader extends RecordReader<Text, DataTuple>{

	private TableInputSplit inputSplit;
	private TaskAttemptContext context;

	private DBInstance instance;
	private BufferPoolManager bpManager;
	private int resourceID;
	private TupleIterator iterator;
	private DataTuple tuple;
	private TablePage tablePage;
	private TableDescriptor table;

	private String tableName;
	private int firstPage;
	private int lastPage;
	private int currentPage;
	private int pageSize;
	private int INPUT_PREFETCH_WINDOW;
	private int prefetchLastPage;


	TableRecordReader(DBInstance instance){
		this.instance = instance;
	}


	@Override
	public void initialize(InputSplit paramInputSplit,
			TaskAttemptContext paramTaskAttemptContext) throws IOException,
			InterruptedException {

//		System.out.println("Initialize table record reader..");

		this.inputSplit = (TableInputSplit) paramInputSplit;
		this.context = paramTaskAttemptContext;

		tableName = inputSplit.getTable();
		firstPage = inputSplit.getFirstPage();
		lastPage = inputSplit.getLastPage();
		pageSize = inputSplit.getPageSize();

		INPUT_PREFETCH_WINDOW = context.getConfiguration().getInt("dbms.input_prefetch_window", 32);

		this.bpManager = instance.getBufferPool();
		this.resourceID = instance.getCatalogue().getTable(tableName).getResourceId();
		table = instance.getCatalogue().getTable(tableName);

		if (firstPage + INPUT_PREFETCH_WINDOW  < lastPage -1){
			prefetchLastPage = firstPage + INPUT_PREFETCH_WINDOW;
		}else{
			prefetchLastPage = lastPage - 1;
		}

		try {
			bpManager.prefetchPages(resourceID, firstPage, prefetchLastPage);
			tablePage = (TablePage) bpManager.getPageAndPin(resourceID, firstPage);
			currentPage = firstPage;
			//TODO to check how many columns
			iterator = tablePage.getIterator(table.getSchema().getNumberOfColumns(), Long.MAX_VALUE);
		} catch (BufferPoolException e) {
			e.printStackTrace();
		} catch (PageExpiredException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageTupleAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			if(iterator.hasNext()){
				this.tuple = iterator.next();
				return true;
			}else{
				if(currentPage < lastPage -1){

					if(currentPage >= prefetchLastPage){
						if (currentPage + INPUT_PREFETCH_WINDOW  < lastPage -1 ){
							prefetchLastPage = currentPage + INPUT_PREFETCH_WINDOW;
						}else{
							prefetchLastPage = lastPage -1 ;
						}
						bpManager.prefetchPages(resourceID, currentPage, prefetchLastPage);
					}

					bpManager.unpinPage(resourceID, currentPage);
					currentPage ++;
					tablePage = (TablePage) bpManager.getPageAndPin(resourceID, currentPage);
					iterator = tablePage.getIterator(table.getSchema().getNumberOfColumns(), Long.MAX_VALUE);
					this.tuple = iterator.next();
					return true;
				}else{
					bpManager.unpinPage(resourceID, currentPage);
					return false;
				}
			}
		} catch (PageTupleAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufferPoolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(this.tableName);
	}

	@Override
	public DataTuple getCurrentValue() throws IOException, InterruptedException {
		return this.tuple;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(this.lastPage == this.firstPage )
			return (float) 1.0;
		return ((float) (this.currentPage - this.firstPage )) / (this.lastPage - this.firstPage);
	}

	@Override
	public void close() throws IOException {
//		this.bpManager.closeBufferPool();

	}


}