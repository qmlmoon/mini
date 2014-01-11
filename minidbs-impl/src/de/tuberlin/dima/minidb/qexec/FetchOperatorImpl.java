package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

public class FetchOperatorImpl implements FetchOperator{
	
	PhysicalPlanOperator child;
	BufferPoolManager bufferPool;
	int tableResourceId;
	int[] outputColumnMap;
	boolean initFlag;
	boolean isFinished;

	public FetchOperatorImpl(
			PhysicalPlanOperator child, 
			BufferPoolManager bufferPool, 
			int tableResourceId, 
			int[] outputColumnMap){
		this.child = child;
		this.bufferPool = bufferPool;
		this.tableResourceId = tableResourceId;
		this.outputColumnMap = outputColumnMap;
		this.initFlag = false;		
	}
	
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		child.open(correlatedTuple);
		initFlag = true;	
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		
		DataTuple dataTuple;
		TablePage tablePage;
		
		while((dataTuple = this.child.next()) != null){
			RID rid = (RID) dataTuple.getField(0);
			try {
				tablePage = (TablePage) this.bufferPool.getPageAndPin(tableResourceId, rid.getPageIndex());
				
				DataTuple originDataTuple = tablePage.getDataTuple(rid.getTupleIndex(), Long.MAX_VALUE, this.outputColumnMap.length);
				
				if(originDataTuple != null){
					
					DataTuple result = new DataTuple(this.outputColumnMap.length);
					for(int i = 0; i < this.outputColumnMap.length; i ++){
						result.assignDataField(originDataTuple.getField(this.outputColumnMap[i]), i);
					}
					
					return result;
					
				}else{
					return null;
				}
				
			} catch (BufferPoolException | IOException | PageExpiredException | PageTupleAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		initFlag = false;		
	}
	
}