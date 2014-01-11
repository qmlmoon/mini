package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

public class InsertOperatorImpl implements InsertOperator{
	
	private BufferPoolManager bufferPoolManager;
	private TableResourceManager tableManager;
	private int resourceId;
	private BTreeIndex[] indexes;
	private int[] columnNumbers;
	private PhysicalPlanOperator child;
	private boolean initFlag;
	private boolean isFinished;
	
	public InsertOperatorImpl(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId, BTreeIndex[] indexes,
			int[] columnNumbers, PhysicalPlanOperator child){
		this.bufferPoolManager = bufferPool;
		this.tableManager = tableManager;
		this.resourceId = resourceId;
		this.indexes = indexes;
		this.columnNumbers = columnNumbers;
		this.child = child;
		initFlag = false;
		isFinished = false;
	}

	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		initFlag = true;
		child.open(correlatedTuple);		
	}

	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		
		if(isFinished){
			return null;
		}
		long count = 0;
		TablePage tablePage = null;
		DataTuple dataTuple = null;

		try {
			tablePage = (TablePage) bufferPoolManager.getPageAndPin(resourceId, tableManager.getLastDataPageNumber());
			
			while( (dataTuple = child.next()) != null ){
				
				//insert tuple
				if( !tablePage.insertTuple(dataTuple) ){
					TablePage newTablePage = (TablePage) bufferPoolManager.createNewPageAndPin(resourceId);
					bufferPoolManager.unpinPage(resourceId, tablePage.getPageNumber());
					if( !newTablePage.insertTuple(dataTuple)){
						throw new QueryExecutionException(
								"A new page is created, but tuple could not be inserted into a new empty page.");
					}
					tablePage = newTablePage;
				}
				
				//insert indexes
				RID rid = new RID(tablePage.getPageNumber(), tablePage.getNumRecordsOnPage()-1);
				
				for(int i = 0; i < indexes.length; i++){
					BTreeIndex index = indexes[i];
					index.insertEntry(dataTuple.getField(this.columnNumbers[i]), rid);					
				}
				
				count++;
			}
			
			this.isFinished = true;
			
			//TODO return what?
			return new DataTuple( new DataField[] { new BigIntField(count) });
			
			
		} catch (BufferPoolException | IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (PageExpiredException | PageFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if (tablePage != null){
				bufferPoolManager.unpinPage(resourceId, tablePage.getPageNumber());
			}
		}
		
		
		
		return null;	
	}

	public void close() throws QueryExecutionException {
		initFlag = false;
		child.close();
		
		
	}
	
}