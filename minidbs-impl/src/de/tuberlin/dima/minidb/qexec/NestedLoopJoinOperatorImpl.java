package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class NestedLoopJoinOperatorImpl implements NestedLoopJoinOperator{

	private PhysicalPlanOperator outerChild;
	private PhysicalPlanOperator innerChild; 
	private JoinPredicate joinPredicate;
	private int[] columnMapOuterTuple;
	private int[] columnMapInnerTuple;
	private boolean initFlag;
	private DataTuple dataTuple;
	
	public NestedLoopJoinOperatorImpl(
			PhysicalPlanOperator outerChild, 
			PhysicalPlanOperator innerChild, 
			JoinPredicate joinPredicate,
			int[] columnMapOuterTuple, 
			int[] columnMapInnerTuple) {
		this.outerChild = outerChild;
		this.innerChild = innerChild;
		this.joinPredicate = joinPredicate;
		this.columnMapOuterTuple = columnMapOuterTuple;
		this.columnMapInnerTuple = columnMapInnerTuple;
		this.initFlag = false;
		this.dataTuple = null;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		initFlag = true;
		this.outerChild.open(correlatedTuple);
		this.dataTuple = this.outerChild.next();
		if(this.dataTuple == null ){
			return;
		}
		this.innerChild.open(dataTuple);
		
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		if(!initFlag){
			throw new QueryExecutionException(
					"Error: next() before open() was called.");
		}
		if(this.dataTuple == null){
			return null;
		}
		DataTuple innerDataTuple;
		
		do {
			while ((innerDataTuple = this.innerChild.next()) == null) {
				this.innerChild.close();
				this.dataTuple = this.outerChild.next();
				if (this.dataTuple == null) {
					return null;
				}

				this.innerChild.open(this.dataTuple);
			}
		}

		while ((this.joinPredicate != null)
				&& (!(this.joinPredicate.evaluate(this.dataTuple, innerDataTuple))));
		
		DataTuple result = new DataTuple(this.columnMapOuterTuple.length);
		
		for (int i = 0; i < this.columnMapOuterTuple.length; ++i) {
			if ( this.columnMapOuterTuple[i] != -1) {
				result.assignDataField(this.dataTuple.getField(this.columnMapOuterTuple[i]), i);
			}
		}
		
		for (int i = 0; i < this.columnMapInnerTuple.length; ++i) {
			if (this.columnMapInnerTuple[i] != -1) {
				result.assignDataField(innerDataTuple.getField(this.columnMapInnerTuple[i]), i);
			}
		}

		return result;
	}

	@Override
	public void close() throws QueryExecutionException {
		this.initFlag = false;
		this.dataTuple = null;
		this.outerChild.close();
		this.innerChild.close();
		
		
	}

	@Override
	public PhysicalPlanOperator getOuterChild() {
		return this.outerChild;
	}

	@Override
	public PhysicalPlanOperator getInnerChild() {
		return  this.innerChild;
	}

	@Override
	public JoinPredicate getJoinPredicate() {
		return this.joinPredicate;
	}
	
}