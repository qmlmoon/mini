package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;

public class IndexCorrelatedLookupOperatorImpl extends IndexLookupOperatorImpl implements IndexCorrelatedLookupOperator{
	
	private int correlatedColumnIndex;
	
	public IndexCorrelatedLookupOperatorImpl(BTreeIndex index, int correlatedColumnIndex){
		super(index, null);
		this.correlatedColumnIndex = correlatedColumnIndex;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		this.lowerBound = correlatedTuple.getField(this.correlatedColumnIndex);
		this.upperBound = correlatedTuple.getField(this.correlatedColumnIndex);
		super.open(correlatedTuple);
		
	}


	@Override
	public void close() throws QueryExecutionException {
		super.close();
		this.lowerBound = null;
		this.upperBound = null;
		
	}
	
}