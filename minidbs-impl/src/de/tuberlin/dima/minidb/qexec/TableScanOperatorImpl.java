package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

public class TableScanOperatorImpl implements TableScanOperator {
	
	BufferPoolManager bufferPool;
	TableResourceManager tableManager;
	int resourceId;
	int[] producedColumnIndexes;
	LowLevelPredicate[] predicate;
	int prefetchWindowLength;
	
	TupleIterator iterator;
	int firstPage, lastPage, currentPage;

	public TableScanOperatorImpl(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) 
	{
		this.bufferPool = bufferPool;
		this.tableManager = tableManager;
		this.resourceId = resourceId;
		this.producedColumnIndexes = producedColumnIndexes;
		this.predicate = predicate;
		this.prefetchWindowLength = prefetchWindowLength;
		
		this.firstPage = this.tableManager.getFirstDataPageNumber();
		this.lastPage = this.tableManager.getLastDataPageNumber();
		this.currentPage = this.firstPage;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		this.firstPage = this.tableManager.getFirstDataPageNumber();
		this.lastPage = this.tableManager.getLastDataPageNumber();
		if(firstPage > lastPage) {
			throw new QueryExecutionException("First Page does not exist!");
		}

		this.currentPage = this.firstPage;
		
		try {
			//prefetch pages
			this.bufferPool.prefetchPages(resourceId, firstPage, firstPage+prefetchWindowLength);

			//TODO: directly cast or new ??
			TablePage page = (TablePage) this.bufferPool.getPageAndPin(resourceId, firstPage);
			this.iterator = page.getIterator(predicate, this.tableManager.getSchema().getNumberOfColumns(), Long.MAX_VALUE);
			
		} catch (BufferPoolException | IOException | 
				PageExpiredException | PageTupleAccessException e) {
			handleException(e);
		} 
	}
	
	private void loadNextPage() throws BufferPoolException, IOException, PageExpiredException, PageTupleAccessException {
		if(this.currentPage == this.lastPage) {
			this.iterator = null;
			return;
		}
		//TODO: directly cast or new ??
		TablePage page = (TablePage) this.bufferPool.unpinAndGetPageAndPin(resourceId, currentPage, currentPage+1);
		currentPage++;
		
		//get a full tuple here, without bitmap
		this.iterator = page.getIterator(predicate, this.tableManager.getSchema().getNumberOfColumns(), Long.MAX_VALUE);

		//prefetch new page
		if(currentPage + prefetchWindowLength <= lastPage) {
			this.bufferPool.prefetchPage(resourceId, currentPage + prefetchWindowLength);
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		DataTuple result = null;
		if(this.iterator != null) {
			try {
				while(this.iterator !=null && !this.iterator.hasNext()) {
					loadNextPage();
				}
				if(this.iterator == null || !this.iterator.hasNext()) {
					return null;
				}

				DataTuple origin = this.iterator.next();

				//get projection of the tuple according to given indexes
				result = new DataTuple(this.producedColumnIndexes.length);
				for(int i=0; i<this.producedColumnIndexes.length; ++i) {
					result.assignDataField(origin.getField(this.producedColumnIndexes[i]), i);
				}
			} catch (PageExpiredException | PageTupleAccessException
					| BufferPoolException | IOException e) {
				handleException(e);
			}
		}
		return result;
	}
	
	private void handleException(Exception e) throws QueryExecutionException {
		this.iterator = null;
		throw new QueryExecutionException(e);
	}

	@Override
	public void close() throws QueryExecutionException {
		this.iterator = null;
		for(int i=this.currentPage; i<=this.lastPage && i<currentPage+prefetchWindowLength; ++i) {
			this.bufferPool.unpinPage(resourceId, i);
		}
	}

}
