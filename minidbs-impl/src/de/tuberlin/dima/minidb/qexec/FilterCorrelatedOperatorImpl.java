package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class FilterCorrelatedOperatorImpl implements FilterCorrelatedOperator{

	protected PhysicalPlanOperator child;
	protected JoinPredicate correlatedPredicate;
	private DataTuple correlatedTuple;
	private boolean initFlag;
	
	
	public FilterCorrelatedOperatorImpl(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
		this.child = child;
		this.correlatedPredicate = correlatedPredicate;
		this.initFlag = false;
		
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		this.initFlag = true;
		this.child.open(correlatedTuple);
		this.correlatedTuple = correlatedTuple;
		
	}


	@Override
	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		
		DataTuple dataTuple;
		
		while((dataTuple = child.next()) != null ){
			if (this.correlatedPredicate == null ){
				return dataTuple;
			}
			else if ( this.correlatedPredicate.evaluate(correlatedTuple, dataTuple)){
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
	public JoinPredicate getCorrelatedPredicate() {
		return this.correlatedPredicate;
	}
	
}