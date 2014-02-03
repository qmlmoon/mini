package de.tuberlin.dima.minidb.optimizer.cost;

import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;

public class CostEstimatorImpl implements CostEstimator {

	@Override
	public long computeTableScanCosts(TableDescriptor table) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeIndexLookupCosts(IndexDescriptor index,
			TableDescriptor baseTable, long cardinality) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeSortCosts(Column[] columnsInTuple, long numTuples) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeFetchCosts(TableDescriptor fetchedTable,
			long cardinality, boolean sequential) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeFilterCost(LocalPredicate pred, long cardinality) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeMergeJoinCost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeNestedLoopJoinCost(long outerCardinality,
			OptimizerPlanOperator innerOp) {
		// TODO Auto-generated method stub
		return 0;
	}

}
