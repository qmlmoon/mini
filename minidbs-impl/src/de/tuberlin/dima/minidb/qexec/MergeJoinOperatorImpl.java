package de.tuberlin.dima.minidb.qexec;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataTuple;

public class MergeJoinOperatorImpl implements MergeJoinOperator {
	private PhysicalPlanOperator leftChild;
	private PhysicalPlanOperator rightChild;
	private int[] leftJoinColumns;
	private int[] rightJoinColumns;
	private int[] columnMapLeftTuple;
	private int[] columnMapRightTuple;
	private List<DataTuple> left = new ArrayList<DataTuple>();
	private List<DataTuple> right = new ArrayList<DataTuple>();
	private int leftNum = 0;
	private int rightNum = 0;
	
	public MergeJoinOperatorImpl(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftJoinColumns = leftJoinColumns;
		this.rightJoinColumns = rightJoinColumns;
		this.columnMapLeftTuple = columnMapLeftTuple;
		this.columnMapRightTuple = columnMapRightTuple;
	}
	
	
	private int checklr(DataTuple t1, DataTuple t2) {
		for (int i = 0; i < this.leftJoinColumns.length; i++) {
			if (t1.getField(this.leftJoinColumns[i]).compareTo(t2.getField(this.rightJoinColumns[i])) < 0 ) 
				return -1;
			if (t1.getField(this.leftJoinColumns[i]).compareTo(t2.getField(this.rightJoinColumns[i])) == 0)
				continue;
			return 1;
		}
		return 0;
	}
	
	private int checkll(DataTuple t1, DataTuple t2) {
		for (int i = 0; i < this.leftJoinColumns.length; i++) {
			if (t1.getField(this.leftJoinColumns[i]).compareTo(t2.getField(this.leftJoinColumns[i])) < 0 ) 
				return -1;
			if (t1.getField(this.leftJoinColumns[i]).compareTo(t2.getField(this.leftJoinColumns[i])) == 0)
				continue;
			return 1;
		}
		return 0;
	}
	
	private int checkrr(DataTuple t1, DataTuple t2) {
		for (int i = 0; i < this.rightJoinColumns.length; i++) {
			if (t1.getField(this.rightJoinColumns[i]).compareTo(t2.getField(this.rightJoinColumns[i])) < 0 ) 
				return -1;
			if (t1.getField(this.rightJoinColumns[i]).compareTo(t2.getField(this.rightJoinColumns[i])) == 0)
				continue;
			return 1;
		}
		return 0;
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		this.leftChild.open(null);
		this.rightChild.open(null);
		DataTuple tmp = this.leftChild.next();
		while (tmp != null) {
			left.add(tmp);
			tmp = this.leftChild.next();
		}
		tmp = this.rightChild.next();
		while (tmp != null) {
			right.add(tmp);
			tmp = this.rightChild.next();
		}
	}

	private DataTuple generateDataTuple(DataTuple t1, DataTuple t2) {
		int tot = 0;
		for (int i = 0; i < columnMapLeftTuple.length; i++)
			if (columnMapLeftTuple[i] != -1)
				tot++;
		for (int i = 0; i < columnMapRightTuple.length; i++)
			if (columnMapRightTuple[i] != -1)
				tot++;
		DataTuple outputTuple = new DataTuple(tot);
		
		for (int i = 0; i < columnMapLeftTuple.length; i++) {
			int index = columnMapLeftTuple[i];
			if (index != -1) {
				outputTuple.assignDataField(t1.getField(index), i);
			}
		}
		for (int i = 0; i < columnMapRightTuple.length; i++) {
			int index = columnMapRightTuple[i];
			if (index != -1) {
				outputTuple.assignDataField(t2.getField(index), i);
			}
		}
		return outputTuple;
	}
	
	@Override
	public DataTuple next() throws QueryExecutionException {
		
			while (true) {
				if (leftNum == left.size() || rightNum == right.size())
					return null;
				if (checklr(left.get(leftNum), right.get(rightNum)) == 0) {
					DataTuple tmp = generateDataTuple(left.get(leftNum),right.get(rightNum));
					
					if ((leftNum + 1 == left.size() ||checkll(left.get(leftNum + 1),left.get(leftNum)) != 0) && 
							(rightNum + 1 == right.size() ||checkrr(right.get(rightNum + 1), right.get(rightNum)) != 0)) {
						leftNum++;
						rightNum++;
					}
					else if (leftNum + 1< left.size() && checkll(left.get(leftNum + 1), left.get(leftNum)) == 0) {
						leftNum++;
					}
					else if (rightNum + 1 < right.size() && checkrr(right.get(rightNum + 1), right.get(rightNum)) == 0) {
						rightNum++;
						leftNum = 0;
						while (checklr(left.get(leftNum), right.get(rightNum)) != 0)
							leftNum++;
					}
					return tmp;
				}
				else if (checklr(left.get(leftNum),right.get(rightNum)) < 0) 
					leftNum++;
				else
					rightNum++;
			}

	}

	@Override
	public void close() throws QueryExecutionException {
		// TODO Auto-generated method stub
		
	}

}
