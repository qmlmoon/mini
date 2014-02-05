package de.tuberlin.dima.minidb.test.mapred.qexec;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.TableInputOperator;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.TableScanOperator;
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
	
	public void testScanOperator(String table, LowLevelPredicate predicate) throws IOException, BufferPoolException, PageExpiredException, PageTupleAccessException, QueryExecutionException {
		// First, open the table to get the descriptor.
		TableDescriptor desc = dbInstance.getCatalogue().getTable(table);
		
		LowLevelPredicate predicates[] = new LowLevelPredicate[1];
		predicates[0] = predicate;
		
		// Build a local query plan.
		int columnIndexes[] = new int[desc.getSchema().getNumberOfColumns()];
		for (int i=0; i<columnIndexes.length; ++i) columnIndexes[i] = i;
		TableScanOperator root = AbstractExtensionFactory.
				getExtensionFactory().createTableScanOperator(
						dbInstance.getBufferPool(), desc.getResourceManager(), 
						desc.getResourceId(), columnIndexes, predicates, 3);
		
		// Now build the Hadoop query plan.
		BulkProcessingOperator op = AbstractExtensionFactory.getExtensionFactory().
				createHadoopTableScanOperator(dbInstance, 
						new TableInputOperator(dbInstance, table), predicate);
				
		Assert.assertTrue(op.run());
		
		Assert.assertTrue(TestUtils.compareTableToQueryPlan(dbInstance, table, root));
	}
	
	@Test
	public void testScanOperatorOnLargeTable() throws IOException, BufferPoolException, PageExpiredException, PageTupleAccessException, QueryExecutionException {
		testScanOperator("lineitem", new LowLevelPredicate(Operator.GREATER, new IntField(200), 0));
	}
}
