package de.tuberlin.dima.minidb.mapred.qexec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class ScanOperator extends HadoopOperator<Text, DataTuple>{

	private BulkProcessingOperator child;
	private LocalPredicate predicate;
	
	public ScanOperator(DBInstance instance, BulkProcessingOperator child, LocalPredicate predicate)
			throws IOException {
		super(instance, child);
		this.child = child;
		this.predicate = predicate;
		configureMapOnlyJob(ScanMapper.class);
		ScanMapper.registerPredicate(job.getConfiguration(), this.predicate);
	}

	@Override
	protected TableSchema createResultSchema() {
		return child.getResultSchema();
	}
	
	public static class ScanMapper extends Mapper<Text, DataTuple, Text, DataTuple>{
		public LocalPredicate predicate;
		
		public static void registerPredicate(Configuration conf, LocalPredicate predicate) throws IOException{
			conf.set("ScanMapper.Predicate", SerializationUtils.writeLocalPredicateToString(predicate));
		}
		
		@Override
		public void setup(Context context) throws IOException{
			predicate = SerializationUtils.readLocalPredicateFromString(context.getConfiguration().get("ScanMapper.Predicate"));
			if(predicate == null){
				throw new RuntimeException("Predicate is noe specified..");
			}
		}
		
		@Override
		public void map(Text text, DataTuple tuple, Context context) {
			try {
				if(predicate == null){
					context.write(text, tuple);
				} 
				else if(predicate.evaluate(tuple)){
					context.write(text, tuple);
				}
			} catch (QueryExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
}