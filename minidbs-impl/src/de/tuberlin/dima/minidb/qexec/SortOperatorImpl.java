package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeapException;

public class SortOperatorImpl implements SortOperator {
	private PhysicalPlanOperator child;
	private QueryHeap queryHeap;
	private DataType[] columnTypes;
	private int estimatedCardinality;
	private int[] sortColumns;
	private boolean[] columnsAscending;
	private int heapId;
	private ExternalTupleSequenceIterator[] it;
	private DataTuple[] ext,internalSort;
	
	public SortOperatorImpl(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		this.child = child;
		this.queryHeap = queryHeap;
		this.columnTypes = columnTypes;
		this.estimatedCardinality = estimatedCardinality;
		this.sortColumns = sortColumns;
		this.columnsAscending = columnsAscending;
		
	}
	
	private boolean check(DataTuple t1, DataTuple t2) {
		for (int i = 0; i < this.sortColumns.length; i++) {
			if ((t1.getField(this.sortColumns[i]).compareTo(t2.getField(this.sortColumns[i])) < 0 && !this.columnsAscending[i]) ||
					(t1.getField(this.sortColumns[i]).compareTo(t2.getField(this.sortColumns[i])) > 0 && this.columnsAscending[i]))
				return true;
			if (t1.getField(this.sortColumns[i]).compareTo(t2.getField(this.sortColumns[i])) == 0)
				continue;
			return false;
		}
		return true;
	}

	private void qsort(int low, int high) {
		int i = low;
		int j = high;
		DataTuple mid = internalSort[(low + high) / 2];
		while (i <= j) {
			while (!check(internalSort[i], mid)) 
				i++;
			while (!check(mid,internalSort[j]))
				j--;
			if (i <= j) {
				DataTuple tmp = internalSort[i];
				internalSort[i] = internalSort[j];
				internalSort[j] = tmp;
				i++;
				j--;
			}
		}
		if (low < j)
			qsort(low, j);
		if (high > i)
			qsort(i, high);
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		try {
			heapId = this.queryHeap.reserveSortHeap(columnTypes, this.estimatedCardinality);
			this.child.open(null);
			int len = this.queryHeap.getMaximalTuplesForInternalSort(heapId);
			
			while (true){
				this.internalSort = this.queryHeap.getSortArray(heapId);
				int count = 0;
				DataTuple t = this.child.next();
				while (t != null && count < len) {
					internalSort[count++] = t;
					t = this.child.next();
				}
				this.qsort(0, count - 1);
				this.queryHeap.writeTupleSequencetoTemp(heapId, internalSort, count);
//				this.queryHeap.releaseSortArray(heapId);
				this.it = this.queryHeap.getExternalSortedLists(heapId);
				if (t == null)
					break;
			}
			ext = new DataTuple[it.length];
			for (int i = 0; i < it.length; i++)
				if (it[i].hasNext())
					ext[i] = it[i].next();
		} catch (QueryHeapException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		try {
			DataTuple min = null;
			int pos = 0;
			for  (int i = 0; i < it.length; i++)
				if (ext[i] != null && min == null) {
					min = ext[i];
					pos = i;
				}
			if (min == null)
				return null;
			for (int i = 0; i < it.length; i++)
				if (check(min, ext[i])) {
					min = ext[i];
					pos = i;
				}
			if (it[pos].hasNext())
				ext[pos] = it[pos].next();
			else
				ext[pos] = null;
			return min;
		} catch (QueryHeapException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		// TODO Auto-generated method stub
		this.queryHeap.releaseSortHeap(heapId);
	}

}
