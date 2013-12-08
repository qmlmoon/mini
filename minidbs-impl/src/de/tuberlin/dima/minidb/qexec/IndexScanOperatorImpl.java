package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class IndexScanOperatorImpl implements IndexScanOperator {
	
	BTreeIndex btree;
	DataField startKey;
	DataField stopKey;
	boolean startKeyIncluded;
	boolean stopKeyIncluded;
	
	IndexResultIterator<DataField> iterator;
	
	public IndexScanOperatorImpl(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		this.btree = index;
		this.startKey = startKey;
		this.stopKey = stopKey;
		this.startKeyIncluded = startKeyIncluded;
		this.stopKeyIncluded = stopKeyIncluded;
		
		this.iterator = null;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		try {
			this.iterator = this.btree.lookupKeys(startKey, stopKey, startKeyIncluded, stopKeyIncluded);
		} catch (IndexFormatCorruptException | PageFormatException
				| IOException e) {
			throw new QueryExecutionException(e);
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		if(this.iterator != null) {
			try {
				if(this.iterator.hasNext()) {
					DataTuple result = new DataTuple(1);
					result.assignDataField(this.iterator.next(), 0);
					return result;
				}
				else {
					return null;
				}
			} catch (IndexFormatCorruptException | IOException
					| PageFormatException e) {
				throw new QueryExecutionException(e);
			}
		}
		else {
			return null;
		}
	}

	@Override
	public void close() throws QueryExecutionException {
		this.iterator = null;
	}

}
