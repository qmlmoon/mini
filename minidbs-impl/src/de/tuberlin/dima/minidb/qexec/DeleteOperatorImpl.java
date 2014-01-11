package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;


public class DeleteOperatorImpl implements DeleteOperator{
	
	BufferPoolManager bufferPoolManager;
	int resourceId;
	PhysicalPlanOperator child;
	boolean initFlag;
	boolean isFinished;

	public DeleteOperatorImpl(BufferPoolManager bufferPool, int resourceId, PhysicalPlanOperator child) {
		this.bufferPoolManager = bufferPool;
		this.resourceId = resourceId;
		this.child = child;
		this.initFlag = false;
		this.isFinished = false;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		this.initFlag = true;
		child.open(correlatedTuple);
		
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		if(this.isFinished){
			return null;
		}
		long count = 0;
		TablePage tablePage = null;
		DataTuple dataTuple = null;
		
		try {
			while( (dataTuple = child.next()) != null){
				RID rid = (RID) dataTuple.getField(0);
			
				tablePage = (TablePage) bufferPoolManager.getPageAndPin(resourceId, rid.getPageIndex() );
				tablePage.deleteTuple(rid.getTupleIndex());
				bufferPoolManager.unpinPage(resourceId, tablePage.getPageNumber());
				count ++;
			}
			
			this.isFinished = true;
			
			return dataTuple;
			
		} catch (BufferPoolException | IOException | PageExpiredException | PageTupleAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(tablePage != null){
				bufferPoolManager.unpinPage(resourceId, tablePage.getPageNumber());
			}
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		initFlag = false;
		child.close();
		
	}
	
}