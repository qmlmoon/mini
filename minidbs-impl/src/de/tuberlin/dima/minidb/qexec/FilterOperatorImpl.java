package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class FilterOperatorImpl implements FilterOperator{
	
	protected PhysicalPlanOperator child;
	protected LocalPredicate predicate;
	private boolean initFlag;

	public FilterOperatorImpl(PhysicalPlanOperator child, LocalPredicate predicate){
		this.child = child;
		this.predicate = predicate;
		initFlag = false;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		initFlag = true;
		this.child.open(correlatedTuple);		
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		
		DataTuple dataTuple;
		
		while((dataTuple = child.next()) != null ){
			if (this.predicate == null ){
				return dataTuple;
			}
			else if ( this.predicate.evaluate(dataTuple)){
				return dataTuple;
			}
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		this.initFlag = false;
		this.child.close();
		
	}

	@Override
	public LocalPredicate getPredicate() {
		return this.predicate;
	}
	
	
}