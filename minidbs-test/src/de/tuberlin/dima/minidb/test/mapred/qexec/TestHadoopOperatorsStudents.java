package de.tuberlin.dima.minidb.test.mapred.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.api.ExtensionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.TableInputOperator;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.solution.GroupByOperatorImpl;
import de.tuberlin.dima.minidb.qexec.solution.SortOperatorImpl;
import de.tuberlin.dima.minidb.qexec.solution.TableScanOperatorImpl;
import de.tuberlin.dima.minidb.test.mapred.TestHadoopIntegrationStudents;
import de.tuberlin.dima.minidb.test.mapred.TestUtils;

/**
 * Test the provided hadoop operators.
 * 
 * @author mheimel
 *
 */
public class TestHadoopOperatorsStudents {

	/**
	 * The path to the file with the configuration.
	 */
	private static final String CONFIG_FILE_NAME = TestHadoopIntegrationStudents.class.getResource("/config.xml").getPath();
	
	/**
	 * The path to the file with the catalogue.
	 */
	private static final String CATALOGUE_FILE_NAME = TestHadoopIntegrationStudents.class.getClass().getResource("/catalogue.xml").getPath();
	
	/**
	 * The database instance to perform the tests on.
	 */
	protected DBInstance dbInstance = null;
	
	@BeforeClass
	public static void initExtensionFactory() throws Exception {
		// initialize the extension factory to have access to the user methods
		AbstractExtensionFactory.initializeDefault();
	}
	
	@Before
	public void setUp() throws Exception {
		// initialize a database instance and start it
		this.dbInstance = new DBInstance(CONFIG_FILE_NAME, CATALOGUE_FILE_NAME);
		int returncode = this.dbInstance.startInstance(); 
		if (returncode != DBInstance.RETURN_CODE_OKAY) {
			throw new Exception("DBInstance could not be started: " + returncode);
		}
	}

	@After
	public void tearDown() throws Exception {
		// check if instance is running
		if (this.dbInstance != null && this.dbInstance.isRunning()) {
			// stop running instance
			this.dbInstance.shutdownInstance(false);
		}
	}
	
	public void testScanOperator(String table, LowLevelPredicate predicate)
			throws IOException, BufferPoolException, PageExpiredException, 
			       PageTupleAccessException, QueryExecutionException {
		// First, open the table to get the descriptor.
		TableDescriptor desc = dbInstance.getCatalogue().getTable(table);
		
		// Build a local query plan.
		int columnIndexes[] = new int[desc.getSchema().getNumberOfColumns()];
		for (int i=0; i<columnIndexes.length; ++i) columnIndexes[i] = i;
		TableScanOperatorImpl referencePlan = new TableScanOperatorImpl(
						dbInstance.getBufferPool(), desc.getResourceManager(), 
						new LowLevelPredicate[] {predicate}, columnIndexes, 
						desc.getResourceId(), 20);
		
		// Now build the Hadoop query plan.
		BulkProcessingOperator op = AbstractExtensionFactory.getExtensionFactory().
				createHadoopTableScanOperator(dbInstance, 
						new TableInputOperator(dbInstance, table), predicate);
				
		Assert.assertTrue(op.run());
		
		TestUtils.compareTableToQueryPlan(
				dbInstance, op.getResultTableName(), referencePlan);
	}
	
	@Test
	public void testScanOperatorOnLargeTable() throws IOException, BufferPoolException, PageExpiredException, PageTupleAccessException, QueryExecutionException {
		testScanOperator("lineitem", 
				new LowLevelPredicate(Operator.GREATER, new IntField(200), 0));
	}
	
	@Test
	public void testScanOperatorOnSmallTable() throws IOException, BufferPoolException, PageExpiredException, PageTupleAccessException, QueryExecutionException {
		testScanOperator("region", 
				new LowLevelPredicate(Operator.GREATER, new IntField(2), 0));
	}
	
	@Test
	public void testGroupByOperator() throws PageExpiredException, PageTupleAccessException, IOException, QueryExecutionException, BufferPoolException {
		TableDescriptor desc = dbInstance.getCatalogue().getTable("customer");

		int[] groupColumns = new int[] {3};
		int[] aggColumns = new int[] {5,5,5,5,5};
		OutputColumn.AggregationType[] aggs = 
				new OutputColumn.AggregationType[] {
					OutputColumn.AggregationType.COUNT,
					OutputColumn.AggregationType.SUM,
					OutputColumn.AggregationType.AVG,
					OutputColumn.AggregationType.MIN,
					OutputColumn.AggregationType.MAX};
		DataType[] aggTypes = new DataType[] {
				DataType.intType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType()};
		int[] groupOutput = new int[] {0, -1, -1, -1, -1, -1};
		int[] aggOutput = new int[] {-1 , 0, 1, 2, 3, 4};
		
		// Set up the reference plan.
		int columnIndexes[] = new int[desc.getSchema().getNumberOfColumns()];
		for (int i=0; i<columnIndexes.length; ++i) columnIndexes[i] = i;
		PhysicalPlanOperator referencePlan = new TableScanOperatorImpl(
				dbInstance.getBufferPool(), desc.getResourceManager(), null, 
				columnIndexes, desc.getResourceId(), 150);
		DataType[] sortSchema = 
				new DataType[desc.getSchema().getNumberOfColumns()];
		for (int i=0; i<sortSchema.length; ++i) {
			sortSchema[i] = desc.getSchema().getColumn(i).getDataType();
		}
		referencePlan = new SortOperatorImpl(referencePlan,
				dbInstance.getQueryHeap(), sortSchema,
				(int) desc.getStatistics().getCardinality(), 
				new int[] {3}, new boolean[] {false});
		referencePlan = new GroupByOperatorImpl(referencePlan,
				new int[] {3}, aggColumns, aggs, aggTypes,
				groupOutput, aggOutput);

		ExtensionFactory ef = new ExtensionFactory();
		// Now set up the Hadoop operator plan.
		BulkProcessingOperator op = ef.
				createHadoopGroupByOperator(
					dbInstance,
					new TableInputOperator(dbInstance, "customer"),
					groupColumns, aggColumns, aggs, aggTypes,
					groupOutput, aggOutput);
		Assert.assertTrue(op.run());
		
		// And compare both plans.
		TestUtils.compareTableToQueryPlan(dbInstance, op.getResultTableName(),
				referencePlan);
	}
}
