package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class IndexLookupOperatorImpl implements IndexLookupOperator{
	
	protected BTreeIndex index;
	protected DataField lowerBound;
	protected boolean lowerIncluded;
	protected DataField upperBound;
	protected boolean upperIncluded;
	protected boolean initFlag;
	protected IndexResultIterator<RID> iter;

	public IndexLookupOperatorImpl(BTreeIndex index, DataField datafield){
		this(index, datafield, true, datafield, true);
	}
	
	public IndexLookupOperatorImpl(
			BTreeIndex index, 
			DataField lowerBound, 
			boolean lowerIncluded, 
			DataField upperBound,
			boolean upperIncluded){
		
		this.index = index;
		this.lowerBound = lowerBound;
		this.lowerIncluded = lowerIncluded;
		this.upperBound = upperBound;
		this.upperIncluded = upperIncluded;
		this.initFlag = false;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		try {
			this.initFlag = true;	

			iter = this.index.lookupRids(this.lowerBound, this.upperBound,
					this.lowerIncluded, this.upperIncluded);
			
			return;
			
		} catch (IndexFormatCorruptException | PageFormatException
				| IOException e) {
			throw new QueryExecutionException();
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		
		try {
			if (this.iter.hasNext()) {
				RID rid = this.iter.next();
				
				return new DataTuple(
						new DataField[] { (DataField) rid });
			}
		} catch (IndexFormatCorruptException | IOException
				| PageFormatException e) {
			throw new QueryExecutionException( e.toString() );
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		this.initFlag = false;

		this.iter = null;
		
	}
	
}