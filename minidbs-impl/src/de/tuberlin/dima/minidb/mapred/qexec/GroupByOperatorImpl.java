package de.tuberlin.dima.minidb.mapred.qexec;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.*;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qml_moon on 21/03/14.
 */
public class GroupByOperatorImpl extends HadoopOperator<DataTuple, DataTuple> {

	private BulkProcessingOperator child;
	private int[] groupColumnIndices;
	private int[] aggColumnIndices;
	private OutputColumn.AggregationType[] aggregateFunctions;
	private DataType[] aggColumnTypes;
	private int[] groupColumnOutputPositions;
	private int[] aggregateColumnOutputPosition;

	public GroupByOperatorImpl(DBInstance instance, BulkProcessingOperator child, int[] groupColumnIndices,
						   int[] aggColumnIndices, OutputColumn.AggregationType[] aggregateFunctions,
						   DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) throws IOException {
		super(instance, child);
		this.child = child;
		this.groupColumnIndices = groupColumnIndices;
		this.aggColumnIndices = aggColumnIndices;
		this.aggregateFunctions = aggregateFunctions;
		this.aggColumnTypes = aggColumnTypes;
		this.groupColumnOutputPositions = groupColumnOutputPositions;
		this.aggregateColumnOutputPosition = aggregateColumnOutputPosition;
		configureMapReduceJob(GroupMapper.class, GroupReducer.class, DataTuple.class, DataTuple.class);
		GroupMapper.register(job.getConfiguration(), this.groupColumnIndices, this.aggColumnIndices);
		GroupReducer.register(job.getConfiguration(), this.aggColumnIndices, this.aggregateFunctions, this.aggColumnTypes,
			this.groupColumnOutputPositions, this.aggregateColumnOutputPosition);
	}

	@Override
	protected TableSchema createResultSchema() {
		TableSchema result = new TableSchema(this.child.getResultSchema().getPageSize());
		for (int i = 0; i < this.groupColumnOutputPositions.length; i++) {
			if (this.groupColumnOutputPositions[i] != -1) {
				result.addColumn(this.child.getResultSchema().getColumn(this.groupColumnIndices[this.groupColumnOutputPositions[i]]));
			} else {
				result.addColumn(ColumnSchema.createColumnSchema(Integer.toString(i), this.aggColumnTypes[this.aggregateColumnOutputPosition[i]]));
			}
		}
		return result;
	}


	public static class GroupMapper extends Mapper<Text, DataTuple, DataTuple, DataTuple> {
		private int[] groupColumnIndices;
		private int[] aggColumnIndices;

		public static void register(Configuration conf, int[] groupColumnIndices, int[] aggColumnIndices) throws IOException{
			conf.set("GroupMapper.groupColumnIndices", SerializationUtils.writeIntArrayToString(groupColumnIndices));
			conf.set("GroupMapper.aggColumnIndices", SerializationUtils.writeIntArrayToString(aggColumnIndices));
		}

		@Override
		public void setup(Context context) throws IOException {
			this.groupColumnIndices = SerializationUtils.readIntArrayFromString(context.getConfiguration().get("GroupMapper.groupColumnIndices"));
			this.aggColumnIndices = SerializationUtils.readIntArrayFromString(context.getConfiguration().get("GroupMapper.aggColumnIndices"));
		}

		@Override
		public void map(Text text, DataTuple dataTuple, Context context) throws IOException {
			DataTuple key = new DataTuple(this.groupColumnIndices.length);
			for (int i = 0; i < this.groupColumnIndices.length; i++) {
				key.assignDataField(dataTuple.getField(this.groupColumnIndices[i]), i);
			}

			DataTuple value = new DataTuple(this.aggColumnIndices.length);
			for (int i = 0; i < this.aggColumnIndices.length; i++) {
				value.assignDataField(dataTuple.getField(this.aggColumnIndices[i]), i);
			}
			try {
				context.write(key, value);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public static class GroupReducer extends Reducer<DataTuple, DataTuple, DataTuple, DataTuple> {
		private int[] aggColumnIndices;
		private OutputColumn.AggregationType[] aggregateFunctions;
		private DataType[] aggColumnTypes;
		private int[] groupColumnOutputPositions;
		private int[] aggregateColumnOutputPosition;

		private List<DataField> agg;
		private int [] avg;

		public static void register(Configuration conf, int[] aggColumnIndices, OutputColumn.AggregationType[] aggregateFunctions,
									DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) throws IOException{
			conf.set("GroupMapper.aggColumnIndices", SerializationUtils.writeIntArrayToString(aggColumnIndices));
			conf.set("GroupMapper.aggregateFunctions", SerializationUtils.writeAggregationTypeArrayToString(aggregateFunctions));
			conf.set("GroupMapper.aggColumnTypes", SerializationUtils.writeDataTypeArrayToString(aggColumnTypes));
			conf.set("GroupMapper.groupColumnOutputPositions", SerializationUtils.writeIntArrayToString(groupColumnOutputPositions));
			conf.set("GroupMapper.aggregateColumnOutputPosition", SerializationUtils.writeIntArrayToString(aggregateColumnOutputPosition));
		}

		@Override
		public void setup(Context context) throws IOException {
			this.aggColumnIndices = SerializationUtils.readIntArrayFromString(context.getConfiguration().get("GroupMapper.aggColumnIndices"));
			this.aggregateFunctions = SerializationUtils.readAggregationTypeArrayFromString(context.getConfiguration().get("GroupMapper.aggregateFunctions"));
			this.aggColumnTypes = SerializationUtils.readDataTypeArrayFromString(context.getConfiguration().get("GroupMapper.aggColumnTypes"));
			this.groupColumnOutputPositions = SerializationUtils.readIntArrayFromString(context.getConfiguration().get("GroupMapper.groupColumnOutputPositions"));
			this.aggregateColumnOutputPosition = SerializationUtils.readIntArrayFromString(context.getConfiguration().get("GroupMapper.aggregateColumnOutputPosition"));
		}


		private void createDataField(DataTuple t) {
			for (int i = 0; i < this.aggregateFunctions.length; i++) {
				OutputColumn.AggregationType type = this.aggregateFunctions[i];
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
					avg[i] = 0;
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
		}

		private void DataTupleAdd(DataTuple t) {
			if (t == null)
				return;
			for (int i = 0; i < this.aggregateFunctions.length; i++) {
				OutputColumn.AggregationType type = this.aggregateFunctions[i];
				if (type == OutputColumn.AggregationType.COUNT) {
					if (!t.getField(i).isNULL()) {
						ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
						a.add(1);
						agg.set(i, (DataField)a);
					}
				}
				if (type == OutputColumn.AggregationType.SUM) {
					if (!t.getField(i).isNULL()) {
						ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
						a.add(t.getField(i));
						agg.set(i, (DataField)a);
					}
				}
				if (type == OutputColumn.AggregationType.AVG) {
					if (!t.getField(i).isNULL()) {
						ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(i));
						a.add(t.getField(i));
						agg.set(i, (DataField)a);
						avg[i]++;
					}
				}

				if (type == OutputColumn.AggregationType.MIN) {
					if (agg.get(i).isNULL() && !t.getField(i).isNULL())
						agg.set(i, t.getField(i));
					else if (!agg.get(i).isNULL() && !t.getField(i).isNULL() && agg.get(i).compareTo(t.getField(i)) > 0)
						agg.set(i, t.getField(i));
				}

				if (type == OutputColumn.AggregationType.MAX) {
					if (agg.get(i).isNULL() && !t.getField(i).isNULL())
						agg.set(i, t.getField(i));
					else if (!agg.get(i).isNULL() && !t.getField(i).isNULL() && agg.get(i).compareTo(t.getField(i)) < 0)
						agg.set(i, t.getField(i));
				}
			}
		}

		@Override
		public void reduce(DataTuple key, Iterable<DataTuple> values, Context context) throws IOException {

			agg = new ArrayList<DataField>();
			avg = new int[100];
			boolean first = true;
			for (DataTuple value:values) {
				if (first) {
					createDataField(value);
					first = false;
				}
				DataTupleAdd(value);
			}

			DataTuple out = new DataTuple(this.groupColumnOutputPositions.length);
			for (int i = 0; i < this.groupColumnOutputPositions.length; i++) {
				if (this.groupColumnOutputPositions[i] != -1) {
					out.assignDataField(key.getField(this.groupColumnOutputPositions[i]), i);
				}
			}

			for (int i = 0; i < this.aggregateColumnOutputPosition.length; i++) {
				int index = this.aggregateColumnOutputPosition[i];
				if (index != -1) {
					if (this.aggregateFunctions[index] == OutputColumn.AggregationType.AVG && avg[index] == 0) {
						out.assignDataField(this.aggColumnTypes[index].getNullValue(), i);
						continue;
					}

					if (this.aggregateFunctions[index] == OutputColumn.AggregationType.AVG) {
						ArithmeticType<DataField> a = DataType.asArithmeticType(agg.get(index));
						a.divideBy(avg[index]);
						agg.set(index, (DataField)a);
					}
					out.assignDataField(agg.get(index), i);
				}
			}

			try {
				context.write(key, out);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
