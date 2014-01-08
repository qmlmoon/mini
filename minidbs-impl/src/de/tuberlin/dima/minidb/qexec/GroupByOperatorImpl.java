package de.tuberlin.dima.minidb.qexec;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.ArithmeticType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;

public class GroupByOperatorImpl implements GroupByOperator {

	private PhysicalPlanOperator child;
	private int[] groupColumnIndices;
	private int[] aggColumnIndices;
	private AggregationType[] aggregateFunctions;
	private DataType[] aggColumnTypes;
	private int[] groupColumnOutputPositions;
	private int[] aggregateColumnOutputPosition;
	private DataTuple first;
	private List<DataField> agg = new ArrayList<DataField>();
	private int avg;
	private boolean tupleNull = false;
	
	public GroupByOperatorImpl(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
			AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
		this.child = child;
		this.groupColumnIndices = groupColumnIndices;
		this.aggColumnIndices = aggColumnIndices;
		this.aggregateFunctions = aggregateFunctions;
		this.aggColumnTypes = aggColumnTypes;
		this.groupColumnOutputPositions = groupColumnOutputPositions;
		this.aggregateColumnOutputPosition = aggregateColumnOutputPosition;
		
	}
	
	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		child.open(null);
		first = child.next();
		if (first == null)
			this.tupleNull = true;
	}


	private boolean check(DataTuple t1, DataTuple t2) {
		for (int i = 0; i < this.groupColumnIndices.length; i++) {
			if (t1.getField(this.groupColumnIndices[i]).compareTo(t2.getField(this.groupColumnIndices[i])) != 0 ) 
				return false;
		}
		return true;
	}
	
	private void createDataField(DataTuple t) {
		for (int i = 0; i < this.aggregateFunctions.length; i++) {
			AggregationType type = this.aggregateFunctions[i];
			if (type == OutputColumn.AggregationType.COUNT) {
				DataType datatype = DataType.intType();
				ArithmeticType<DataField> zero = datatype.createArithmeticZero();
				agg.add((DataField)zero);
			}
			if (type == OutputColumn.AggregationType.SUM) {
				DataType datatype = this.aggColumnTypes[i];
				ArithmeticType<DataField> zero = datatype.createArithmeticZero();
				agg.add((DataField)zero);

			}
			if (type == OutputColumn.AggregationType.AVG) {
				this.avg = 0;
				DataType datatype = this.aggColumnTypes[i];
				ArithmeticType<DataField> zero = datatype.createArithmeticZero();
				agg.add((DataField)zero);
			}
			if (type == OutputColumn.AggregationType.MIN) {
				agg.add(this.aggColumnTypes[i].getNullValue());
			}
			if (type == OutputColumn.AggregationType.MAX) {
				agg.add(this.aggColumnTypes[i].getNullValue());
			}
		}

		DataTupleAdd(t);
	}
	
	private void DataTupleAdd(DataTuple t) {
		if (t == null)
			return;
		for (int i = 0; i < this.aggregateFunctions.length; i++) {
			AggregationType type = this.aggregateFunctions[i];
			if (type == OutputColumn.AggregationType.COUNT) {
				int index = this.aggColumnIndices[i];
				if (!t.getField(index).isNULL()) {
					ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
					a.add(1);
					agg.set(i, (DataField)a);
				}
			}
			if (type == OutputColumn.AggregationType.SUM) {
				int index = this.aggColumnIndices[i];
				if (!t.getField(index).isNULL()) {
					ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
					a.add(t.getField(index));
					agg.set(i, (DataField)a);
				}
			}
			if (type == OutputColumn.AggregationType.AVG) {
				int index = this.aggColumnIndices[i];
				if (!t.getField(index).isNULL()) {
					ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
					a.add(t.getField(index));
					agg.set(i, (DataField)a);
					this.avg++;
				}
			}
			
			if (type == OutputColumn.AggregationType.MIN) {
				int index = this.aggColumnIndices[i];
				if (agg.get(i).isNULL() && !t.getField(index).isNULL())
					agg.set(i, t.getField(index));
				else if (!agg.get(i).isNULL() && !t.getField(index).isNULL() && agg.get(i).compareTo(t.getField(index)) > 0)
					agg.set(i, t.getField(index));
			}
			
			if (type == OutputColumn.AggregationType.MAX) {
				int index = this.aggColumnIndices[i];
				if (agg.get(i).isNULL() && !t.getField(index).isNULL())
					agg.set(i, t.getField(index));
				else if (!agg.get(i).isNULL() && !t.getField(index).isNULL() && agg.get(i).compareTo(t.getField(index)) < 0)
					agg.set(i, t.getField(index));
			}
		}
	}
	
	private DataTuple generateDataTuple(DataTuple t) {
		int tot = 0;
		for (int i = 0; i < this.groupColumnOutputPositions.length; i++) {
			int index = this.groupColumnOutputPositions[i];
			if (index != -1)
				tot++;
		}
		for (int i = 0; i < this.aggregateColumnOutputPosition.length; i++) {
			int index = this.aggregateColumnOutputPosition[i];
			if (index != -1) 
				tot++;
		}
		DataTuple outputTuple = new DataTuple(tot);
		for (int i = 0; i < this.groupColumnOutputPositions.length; i++) {
			int index = this.groupColumnOutputPositions[i];
			if (index != -1) {
				outputTuple.assignDataField(t.getField(this.groupColumnIndices[index]), i);
			}
		}
		for (int i = 0; i < this.aggregateColumnOutputPosition.length; i++) {
			int index = this.aggregateColumnOutputPosition[i];
			if (index != -1) {
				if (this.aggregateFunctions[index] == OutputColumn.AggregationType.AVG && this.avg == 0) {
					outputTuple.assignDataField(this.aggColumnTypes[index].getNullValue(), i);
					continue;
				}
					
				if (this.aggregateFunctions[index] == OutputColumn.AggregationType.AVG) {
					ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(index));
					a.divideBy(this.avg);
					agg.set(index, (DataField)a);
				}
				outputTuple.assignDataField(agg.get(index), i);
			}
		}
		return outputTuple;
	}
	
	@Override
	public DataTuple next() throws QueryExecutionException {
		if (this.tupleNull) {
			if (this.groupColumnIndices.length != 0)
				return null;
			else {
				this.tupleNull = false;
				createDataField(null);
				return generateDataTuple(null);
			}
		}
		
		if (first == null)
			return null;
		agg = new ArrayList<DataField>();
		createDataField(first);
		DataTuple second = child.next();
		while (second != null && check(first, second)) {
			DataTupleAdd(second);
			second = child.next();
		}

		DataTuple tmp = first;
		first = second;
		return generateDataTuple(tmp);
	}

	@Override
	public void close() throws QueryExecutionException {
		// TODO Auto-generated method stub
		
	}

}
