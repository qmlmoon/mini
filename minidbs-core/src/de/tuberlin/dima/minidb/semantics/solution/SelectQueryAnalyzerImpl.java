package de.tuberlin.dima.minidb.semantics.solution;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.InternalOperationFailure;
import de.tuberlin.dima.minidb.parser.IntegerLiteral;
import de.tuberlin.dima.minidb.parser.Literal;
import de.tuberlin.dima.minidb.parser.OrderColumn;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.ParseTreeNode;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.parser.RealLiteral;
import de.tuberlin.dima.minidb.parser.SelectQuery;
import de.tuberlin.dima.minidb.parser.StringLiteral;
import de.tuberlin.dima.minidb.parser.TableReference;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Order;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.QuerySemanticsInvalidException;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.SelectQueryAnalyzer;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.PredicateTruth;
import de.tuberlin.dima.minidb.util.Pair;


/**
 * The implementation of the semantical analysis functions for the query compiler. This
 * class' central methods accept a query parse tree and perform some actions based on query
 * semantics, producing a pre-processed query that can be handed to the optimizer.
 * 
 * The following semantical actions are performed:
 * <ul>
 *   <li>Checking for existence of all involved tables and columns.</li>
 *   <li>Checking for compatibility between columns that are involved in
 *       join predicates.</li>
 *   <li>Checking for compatibility of columns and literals in local predicates.</li>
 *   <li>Checking for validity of types in arithmetics (like aggregations).</li>
 *   <li>Adding of transitive join predicates.</li>
 *   <li>Adding of transitive local predicates as implied by join predicates.</li>
 *   <li>Normalization of local predicates, including basic reasoning about their
 *       truth.</li>
 * </ul>
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class SelectQueryAnalyzerImpl implements SelectQueryAnalyzer
{
	// --------------------------------------------------------------------------------------------
	//                           Static utilities across all instances.
	// --------------------------------------------------------------------------------------------
	
	/**
	 * For each data type, a list of allowed literal types.
	 */
	private static final List<List<Class<? extends Literal>>> allowedLiterals = 
		new ArrayList<List<Class<? extends Literal>>>(12);
	
	static {
		// initialize the allowed literals
		List<Class<? extends Literal>> ints = new ArrayList<Class<? extends Literal>>(1);
		ints.add(IntegerLiteral.class);
		// allow integers to be computed with real literals (needs casting operation later on)
		ints.add(RealLiteral.class);
		
		List<Class<? extends Literal>> reals = new ArrayList<Class<? extends Literal>>(2);
		reals.add(IntegerLiteral.class);
		reals.add(RealLiteral.class);
		
		List<Class<? extends Literal>> strings = new ArrayList<Class<? extends Literal>>(1);
		strings.add(StringLiteral.class);
		
		// NOTE: THIS ONE IS DEPENDENT ON THE ORDER OF THE ENUMERATION ELEMENTS
		allowedLiterals.add(ints); // small int
		allowedLiterals.add(ints); // int
		allowedLiterals.add(ints); // big int
		
		allowedLiterals.add(reals); // float
		allowedLiterals.add(reals); // double
		
		allowedLiterals.add(strings); // char
		allowedLiterals.add(strings); // varchar
		allowedLiterals.add(strings); // date
		allowedLiterals.add(strings); // time
		allowedLiterals.add(strings); // timestamp
	};
	
	/**
	 * The comparator for predicate normalization sorts.
	 */
	private static final Comparator<LocalPredicate> PREDICATE_SORTER = new PredicateSorter();
	
	/**
	 * The comparator used to add transitive edges.
	 */
	private static final Comparator<JoinGraphEdge> JOIN_EDGE_SORTER = new JoinEdgeSorter();
	
	
	
	// --------------------------------------------------------------------------------------------
	//                                    Set Up
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * Creates a new semantical analyzer using the schema in the given catalogue.
	 */
	public SelectQueryAnalyzerImpl()
	{
		// do nothing
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                               Main analysis methods 
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * Analyzes and processes a select query semantically and creates the 
	 * representation that is the input to the optimizer later.
	 * 
	 * Among those steps are validation that all referenced tables and columns exist,
	 * normalization and reasoning about predicates (always true / always false),
	 * creation of the join graph, verification that it is complete.
	 *  
	 * @param query The query to be semantically processed.
	 * @return The analyzed and pre-processed query.
	 * @throws QuerySemanticsInvalidException Thrown, if the query is semantically incorrect
	 *                                        or if the query's semantics are not supported.
	 */
	@Override
	public AnalyzedSelectQuery analyzeQuery(SelectQuery query, Catalogue catalogue)
	throws QuerySemanticsInvalidException
	{
		int relationNumber = 0;
		return analyzeQuery(query, catalogue, relationNumber).getFirst();
	}
	
	/**
	 * Internal function to analyze the select query.
	 * 
	 * @param query The query to be semantically processed.
	 * @param catalogue The catalogue used for matching identifiers.
	 * @param relationNumber The current index to use for new relations in the join graph.
	 * @return The analyzed and pre-processed query.
	 * @throws QuerySemanticsInvalidException Thrown, if the query is semantically incorrect
	 *                                        or if the query's semantics are not supported.
	 */
	private Pair<AnalyzedSelectQuery, Integer> analyzeQuery(SelectQuery query, Catalogue catalogue, int relationNumber)
	throws QuerySemanticsInvalidException
	{	
		// sanity checks
		if (query == null) 
		{
			throw new NullPointerException("Query object must not be null");
		}
		if (query.getSelectClause() == null) 
		{
			throw new QuerySemanticsInvalidException("The given query has no 'SELECT' clause.");
		}
		if (query.getFromClause() == null) 
		{
			throw new QuerySemanticsInvalidException("The given query has no 'FROM' clause.");
		}
		
		// -------------------------------------------------------------------------------
		//                                   Tables
		//
		// Collect all tables that are referenced in the FROM clause and check them.
		// -------------------------------------------------------------------------------
		
		Map<String, Relation> tables = new HashMap<String, Relation>();
		
		// build a map from the table alias names to the tables with their descriptors
		Iterator<TableReference> tablesIter = query.getFromClause().getChildren();
		while (tablesIter.hasNext()) 
		{
			TableReference ref = tablesIter.next();
			Relation src = null;
			
			// check that the table exists
			String alias = ref.getAliasName().toLowerCase(Constants.CASE_LOCALE);
			if(ref.getSelectQuery() != null)
			{	
				Pair<AnalyzedSelectQuery, Integer> pair = analyzeQuery(ref.getSelectQuery(), catalogue, relationNumber);
				src = pair.getFirst();
				relationNumber = pair.getSecond();
			}
			else {
				TableDescriptor td = catalogue.getTable(ref.getTableName());
				if (td == null) {
					throw new QuerySemanticsInvalidException("Table '" + ref.getTableName() +
							"' does not exist.");
				}				
				src = new BaseTableAccess(td);
			}
			src.setID(relationNumber++);

			// add to the map
			Relation old = tables.put(alias, src);
			if (old != null) {
				throw new QuerySemanticsInvalidException("Table alias '" + alias +
						"' is used multiple times.");
			}
			
		}
		tablesIter = null;
				
		// -------------------------------------------------------------------------------
		//                                 Predicates
		//
		// Take all predicates from the 'WHERE' clause. Local predicates are directly
		// added to the tables, join predicates are normalized and collected.
		// -------------------------------------------------------------------------------
		
		// the set for join edges
		List<JoinGraphEdge> joinEdges = null;
		// mapping from a hash over table and column to the join edges
		Map<Long, List<TableOpAtomPair>> cols2jointargets = null;
		
		if (query.getWhereClause() != null) {
			joinEdges = new ArrayList<JoinGraphEdge>();
			cols2jointargets = new HashMap<Long, List<TableOpAtomPair>>();
			
			// check the predicates
			// local predicates are associated with the the table they belong to
			Iterator<Predicate> predicateIter = query.getWhereClause().getChildren();
			while (predicateIter.hasNext()) 
			{
				Predicate pred = predicateIter.next();
				
				if (pred.getType() == Predicate.PredicateType.COLUMN_LITERAL) 
				{
					// --------------------------------------------------------
					//                      local predicate
					// --------------------------------------------------------
					
					// check that the table is known and the column exists					
					Relation src = tables.get(pred.getLeftHandColumn().getTableAlias().toLowerCase(Constants.CASE_LOCALE));
					if (src == null) 
					{
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Table alias '" + pred.getLeftHandColumn().getTableAlias() +
								"' is not defined");
					}					
					Column column = src.getColumn(pred.getLeftHandColumn().getColumnName());					
					if (column == null) 
					{
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Column '" + pred.getLeftHandColumn().getColumnName() +
								"' is not defined in this table.");
					}

					// get column and type
					DataType columnType = column.getDataType();
					
					// check that the literal is compatible with the data type
					Literal lit = pred.getRightHandLiteral();
					if (! allowedLiterals.get(columnType.getBasicType().ordinal()).contains(lit.getClass())) {
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Column '" + pred.getLeftHandColumn().getColumnName() + 
								"' is of data type '" + columnType.getBasicType() +
								"' and may not be compared against a literal of type '" +
								lit.getNodeName() + "'.");
					}
					
					try 
					{
						// build an optimizer predicate out of the parsed predicate
						Column col = null;
						// if src is an analyzed query the column has to be rewired
						if(src instanceof AnalyzedSelectQuery)
						{
							// set index of the 
							col = ((AnalyzedSelectQuery) src).getOutputColumns()[column.getColumnIndex()];
						}
						else 
						{
							col = new Column(src, column.getDataType(), column.getColumnIndex());
						}
						LocalPredicateAtom predAtom = new LocalPredicateAtom(pred, col, column.getDataType().getFromString(lit.getLiteralValue()));
						
						// make that predicate part of the table scans predicates
						LocalPredicate currentPred = src.getPredicate();
						if (currentPred == null) {
							src.setPredicate(predAtom);
						}
						else if (currentPred instanceof LocalPredicateAtom) {
							LocalPredicateConjunct conj = new LocalPredicateConjunct();
							conj.addPredicate(currentPred);
							conj.addPredicate(predAtom);
							src.setPredicate(conj);
						}
						else if (currentPred instanceof LocalPredicateConjunct) {
							((LocalPredicateConjunct) currentPred).addPredicate(predAtom);
						}
						else {
							throw new InternalOperationFailure("An unrecognized type of optimizer " +
									"predicate was found during the semantical checks", false, null);
						}
					}
					catch (DataFormatException dfex) {
						throw new QuerySemanticsInvalidException("In predicate \"" + 
								pred.getNodeContents() + "\": The literal '" + lit.getNodeContents() +
								"' is not valid for the type of the column (" + column.getDataType());
					}
				} // end if local predicate
				else if (pred.getType() == Predicate.PredicateType.COLUMN_COLUMN) 
				{
					// --------------------------------------------------------
					//                      join predicate
					// --------------------------------------------------------

					de.tuberlin.dima.minidb.parser.Column leftCol = pred.getLeftHandColumn();
					de.tuberlin.dima.minidb.parser.Column rightCol = pred.getRightHandColumn();
					
					// get the tables that produce this join column and check for their existence
					Relation leftSrc = tables.get(leftCol.getTableAlias().
							toLowerCase(Constants.CASE_LOCALE));
					Relation rightSrc = tables.get(rightCol.getTableAlias().
							toLowerCase(Constants.CASE_LOCALE));
					
					if (leftSrc == null) {
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Table alias '" + pred.getLeftHandColumn().getTableAlias() +
								"' is not defined");
					}
					if (rightSrc == null) {
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Table alias '" + pred.getRightHandColumn().getTableAlias() +
								"' is not defined");
					}
					// no predicates with columns of the same table allowed
					if (rightSrc == leftSrc) 
					{
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Local predicate comparing columns in " +
										"the same table is not supported.");
					}
					
					// make sure the left and right table are set up such that the left one is the one
					// with the lower ID. That makes disambiguation later a lot easier
					if (leftSrc.getID() > rightSrc.getID()) {
						// switch
						de.tuberlin.dima.minidb.parser.Column tempCol = leftCol;
						leftCol = rightCol;
						rightCol = tempCol;
						ParseTreeNode tmpExpr = pred.getLeftHandExpression(); 
						pred.setLeftHandSide(leftCol, pred.getRightHandExpression());
						pred.setRightHandSide(rightCol, tmpExpr);
						
						Relation tempSrc = leftSrc;
						leftSrc = rightSrc;
						rightSrc = tempSrc;
					}
					
					// get the column in those base tables and check for existence
					Column columnLeft = leftSrc.getColumn(pred.getLeftHandColumn().getColumnName());					
					if (columnLeft == null) 
					{
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Column '" + pred.getLeftHandColumn().getColumnName() +
								"' is not defined in this table.");
					}
					Column columnRight = rightSrc.getColumn(pred.getRightHandColumn().getColumnName());					
					if (columnRight == null) 
					{
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Column '" + pred.getLeftHandColumn().getColumnName() +
								"' is not defined in this table.");
					}
					// check for the compatibility of the data types
					if (columnLeft.getDataType() != columnRight.getDataType())
					{
						// data types do not match. throw an error since we have no cast functions
						throw new QuerySemanticsInvalidException("In predicate \"" +
								pred.getNodeContents() + "\": Column '" + rightCol.getColumnName() +
								"' is of a different data type than column '" + 
								leftCol.getColumnName() + "'.");
					}
					// -------------------------------------------------------------
					//  Checks for existence and compatibility are done.
					//  Not create edges, map the columns and disambiguate
					// -------------------------------------------------------------
					Column leftColumn = new Column(leftSrc, columnLeft.getDataType(), columnLeft.getColumnIndex());
					Column rightColumn = new Column(rightSrc, columnRight.getDataType(), columnRight.getColumnIndex());
					// create an optimizer join predicate for this predicate
					JoinPredicateAtom predAtom = new JoinPredicateAtom(
							pred, leftSrc, rightSrc, leftColumn, rightColumn);
					// get the other targets from the map (table,column -> targets)
					Long lHash = (((long) leftSrc.hashCode()) << 32) | columnLeft.getColumnIndex();
					Long rHash = (((long) rightSrc.hashCode()) << 32) | columnRight.getColumnIndex();
					List<TableOpAtomPair> leftTarget = cols2jointargets.get(lHash);
					List<TableOpAtomPair> rightTarget = cols2jointargets.get(rHash);
					// check if this is a duplicate predicate. that is the case, if both
					// target lists exist and at least the right one is already contained
					// in the left one
					boolean duplicate = false;
					if (leftTarget != null && rightTarget != null) {
						for (int i = 0; i < leftTarget.size(); i++) {
							if (leftTarget.get(i).getPop() == rightSrc) {
								// only mark as duplicate if there are no expressions involved
								if(leftTarget.get(i).getPredicate().getParsedPredicate().getLeftHandExpression() == leftTarget.get(i).getPredicate().getParsedPredicate().getLeftHandColumn() &&
										leftTarget.get(i).getPredicate().getParsedPredicate().getRightHandExpression() == leftTarget.get(i).getPredicate().getParsedPredicate().getRightHandColumn() &&
										predAtom.getParsedPredicate().getLeftHandExpression() == predAtom.getParsedPredicate().getLeftHandColumn() &&
										predAtom.getParsedPredicate().getRightHandExpression() == predAtom.getParsedPredicate().getRightHandColumn()
								)
								{
									duplicate = true;
									break;
								}
							}
						}
					}
					if (duplicate) {
						continue;
					}
					// add to the map as new targets 
					if (leftTarget == null) {
						leftTarget = new ArrayList<TableOpAtomPair>();
						cols2jointargets.put(lHash, leftTarget);
					}
					if (rightTarget == null) {
						rightTarget = new ArrayList<TableOpAtomPair>();
						cols2jointargets.put(rHash, rightTarget);
					}
					leftTarget.add(new TableOpAtomPair(rightSrc, predAtom));
					rightTarget.add(new TableOpAtomPair(leftSrc, predAtom));
					
					
					// create an edge for the join graph
					JoinGraphEdge edge = new JoinGraphEdge(leftSrc, rightSrc, predAtom);
					int contained = joinEdges.indexOf(edge);
					if (contained == -1) {
						// not contained, add this edge
						joinEdges.add(edge);
					}
					else {
						// already contains an edge for these two tables
						// merge the predicates
						JoinGraphEdge previousEdge = joinEdges.get(contained);
						JoinPredicate prevPred = previousEdge.getJoinPredicate();
						if (prevPred instanceof JoinPredicateAtom) {
							JoinPredicateConjunct conj = new JoinPredicateConjunct();
							conj.addJoinPredicate(prevPred);
							conj.addJoinPredicate(predAtom);
							previousEdge.setJoinPredicate(conj);
						}
						else if (prevPred instanceof JoinPredicateConjunct) {
							((JoinPredicateConjunct) prevPred).addJoinPredicate(predAtom);
						}
						else {
							throw new InternalOperationFailure("An unrecognized type of optimizer " +
									"predicate was found during the semantical checks", false, null);
						}
					}
					
				} // end if join predicate
				else {
					throw new InternalOperationFailure(
							"Invalid type of predicate in the WHERE clause", false, null);
				}
				
			} // end iterator
		}
		else {
			// no predicates
			joinEdges = Collections.emptyList();
		}
	
		// -------------------------------------------------------------------------------
		//                                  Group By
		//
		// Collect all columns in the 'GROUP BY' clause and check them.  
		// -------------------------------------------------------------------------------
		
		List<ProducedColumn> groupingColumns = null;
		
		if (query.getGroupByClause() != null) {
			groupingColumns = new ArrayList<ProducedColumn>(
					query.getGroupByClause().getNumberOfChildren());
			Iterator<de.tuberlin.dima.minidb.parser.Column> groupColumns = query.getGroupByClause().getChildren();
			while (groupColumns.hasNext()) {
				// get the next column
				de.tuberlin.dima.minidb.parser.Column col = groupColumns.next();
				
				// check whether the column exists
				Relation src = tables.get(col.getTableAlias().
						toLowerCase(Constants.CASE_LOCALE));
				if (src == null) {
					throw new QuerySemanticsInvalidException("Table alias '" + col.getTableAlias() +
							"' is not defined, as referenced in GROUP BY clause: \"" +
							col.getNodeContents() + "\".");
				}
				// check if the referenced column exists
				Column column = src.getColumn(col.getColumnName());					
				if (column == null) 
				{
					throw new QuerySemanticsInvalidException("Column '" + col.getColumnName() + "' in GROUP BY " +
							" is not defined in this table.");
				}
				// check if column is contained in select clause
				Iterator<OutputColumn> columnsIter = query.getSelectClause().getChildren();
				boolean found = false;
				while (columnsIter.hasNext()) {
					OutputColumn oCol = columnsIter.next();
					if (oCol.getColumn().getColumnName().toLowerCase(Constants.CASE_LOCALE).equals(col.getColumnName().toLowerCase(Constants.CASE_LOCALE)))
					{
						found = true;
					}
				}
				if (!found)
				{
					throw new QuerySemanticsInvalidException("Column '" + col.getColumnName() + "' in GROUP BY clause is not contained in SELECT clause.");
				}
				// remember this column (as by its source and column index)
				groupingColumns.add(new ProducedColumn(src, column.getDataType(), column.getColumnIndex(),
						"", null, OutputColumn.AggregationType.NONE));
			}
		}
		else {
			groupingColumns = Collections.emptyList();
		}

		// -------------------------------------------------------------------------------
		//                               Output Columns
		// -------------------------------------------------------------------------------
		
		ProducedColumn[] outputColumns = new ProducedColumn[query.getSelectClause().getNumberOfChildren()];
		Map<String, Integer> colNamePosMap = new HashMap<String, Integer>(
				query.getSelectClause().getNumberOfChildren());
		
		// remember not grouped and aggregated occurrence for checks later on
		boolean foundAggregated = false;
		boolean foundGrouped = false;
		boolean foundAsIs = false;
		
		// now check that the selected columns exist in those tables
		Iterator<OutputColumn> columnsIter = query.getSelectClause().getChildren();
		while (columnsIter.hasNext()) {
			OutputColumn oCol = columnsIter.next();
			
			// check if the referenced table exists
			de.tuberlin.dima.minidb.parser.Column col = oCol.getColumn();
			Relation src = tables.get(col.getTableAlias().
					toLowerCase(Constants.CASE_LOCALE));
			if (src == null) {
				throw new QuerySemanticsInvalidException("Table alias '" + col.getTableAlias() +
						"' is not defined, as referenced in output column \"" +
						oCol.getNodeContents() + "\".");
			}
			// check if the referenced column exists
			Column column = src.getColumn(col.getColumnName());					
			if (column == null) 
			{
				throw new QuerySemanticsInvalidException("Column '" + col.getColumnName() +
						"' is not defined " + 
						"as referenced in output column \"" + oCol.getNodeContents() + "\".");
			}
			
			// check if this column is grouped
			boolean grouped = false;
			for (int i = 0; i < groupingColumns.size(); i++) {
				ProducedColumn gpc = groupingColumns.get(i);
				if (gpc.getRelation() == src && gpc.getColumnIndex() == column.getColumnIndex()) {
//					groupingColumns.remove(i);
					grouped = true;
					break;
				}
			}
			foundGrouped |= grouped;
			
			DataType colType = column.getDataType();
			DataType outputType = colType;
			
			// check if the function is aggregated and if it is a valid aggregate for the data type
			OutputColumn.AggregationType aggType = oCol.getAggType();
			if (aggType != OutputColumn.AggregationType.NONE) {
				foundAggregated = true;
				// allowed in new version SUM(a) GROUP BY a
//				if (grouped) {
//					throw new QuerySemanticsInvalidException("Column '" + col.getNodeContents() + 
//							"' is both aggregated and appears in the GROUP BY clause.");
//				}				
				if (aggType == OutputColumn.AggregationType.SUM || 
					aggType == OutputColumn.AggregationType.AVG)
				{
					// sum and average can only be computed over arithmetic types (integers, floating point numbers)
					if (!colType.isArithmeticType()) {
						throw new QuerySemanticsInvalidException("The aggregate function '" + 
								aggType.name() + "' in column '" + oCol.getNodeContents() + 
								"' is not applicable to the column's data type (" + 
								colType + ")");
					}
				}
				else if (aggType == OutputColumn.AggregationType.COUNT) {
					outputType = DataType.intType();
				}
			}
			else {
				// remember if we found a column that was neither grouped nor aggregated
				foundAsIs |= !grouped;
			}
			
			// create the new column
			int pos = colNamePosMap.size();
			String name = oCol.getResultColumnName();
			ProducedColumn pc = new ProducedColumn(src, outputType, column.getColumnIndex(),
					name, oCol, oCol.getAggType());
			outputColumns[pos] = pc;
			
			// remember the position and check for double use of the output name
			Integer oldPos = colNamePosMap.put(
					oCol.getResultColumnName().toLowerCase(Constants.CASE_LOCALE),
					new Integer(pos));
			if (oldPos != null) {
				throw new QuerySemanticsInvalidException("The column alias name '" + name +
						"' is used twice in the SELECT clause '" + 
						query.getSelectClause().getNodeContents() + "'.");
			}
		}
		columnsIter = null;
		
		// check if query is valid with respect to grouping and aggregation rules
		if (foundAggregated && foundAsIs) {
			throw new QuerySemanticsInvalidException("The SELECT clause '" + 
					query.getSelectClause().getNodeContents() +
					"' contains aggregated column. All non-aggregated columns must occur in the " +
					"GROUP BY clause.");
		}
		else if (foundGrouped && foundAsIs) {
			throw new QuerySemanticsInvalidException("The SELECT clause '" + 
					query.getSelectClause().getNodeContents() +
					"' contains grouped columns, as specified in the GROUP BY clause '" +
					query.getGroupByClause().getNodeContents() + "'. All non-grouped columns " + 
					"must be aggregated.");
		}
	
		// -------------------------------------------------------------------------------
		//                                  Having
		// -------------------------------------------------------------------------------
		
		LocalPredicate havingPred = null;
		
		if (query.getHavingClause() != null) {
			if (!(foundGrouped | foundAggregated)) {
				throw new QuerySemanticsInvalidException("Query has a HAVING clause without " +
						"perfoming any aggregation.");
			}
			
			Iterator<Predicate> havingIter = query.getHavingClause().getChildren();
			while (havingIter.hasNext()) {
				Predicate pred = havingIter.next();
				
				// sanity check
				if (pred.getType() != Predicate.PredicateType.ALIASCOLUMN_LITERAL) {
					throw new InternalOperationFailure("Wrong type of predicate detected in the " +
							"parse tree of the HAVING clause.", false, null);
				}
				
				// get the column
				Integer index = colNamePosMap.get(pred.getLeftHandAliasColumn().
						getResultColumnName().toLowerCase(Constants.CASE_LOCALE));
				if (index == null) {
					throw new QuerySemanticsInvalidException("The column with output name '" + 
							pred.getLeftHandAliasColumn().getResultColumnName() +
							"', referenced in the HAVING clause predicate '" + 
							pred.getNodeContents() + "', is not defined in the SELECT clause.");
				}
				
				ProducedColumn pc = outputColumns[index.intValue()];
				DataType columnType = pc.getOutputDataType();
				
				// check that the literal is compatible with the data type
				Literal lit = pred.getRightHandLiteral();
				if (!allowedLiterals.get(columnType.getBasicType().ordinal()).contains(lit.getClass())) {
					throw new QuerySemanticsInvalidException("In HAVING clause predicate '" +
							pred.getNodeContents() + "': The result column '" + pc.getColumnAliasName() +
							"' is of data type '" + columnType +
							"' and may not be compared against a literal of type '" +
							lit.getNodeName() + "'.");
				}
				
				// parse the literal
				DataField literal = null;
				try 
				{
					 literal = columnType.getFromString(lit.getLiteralValue());
				}
				catch (DataFormatException dfex) {
					throw new QuerySemanticsInvalidException("In HAVING clause predicate '" + 
							pred.getNodeContents() + "': The literal '" + lit.getNodeContents() +
							"' is not valid for the type of the column (" + columnType.getBasicType());
				}
				
				if (pc.getAggregationFunction() == OutputColumn.AggregationType.NONE)	{
					// predicate is on grouping column and will be added to the base table
					Relation target = pc.getRelation();					
					LocalPredicateAtom predAtom = new LocalPredicateAtom(
							pred, pc, literal);
					
					// make that predicate part of the table scans predicates
					LocalPredicate currentPred = target.getPredicate();
					if (currentPred == null) {
						target.setPredicate(predAtom);
					}
					else if (currentPred instanceof LocalPredicateConjunct) {
						((LocalPredicateConjunct) currentPred).addPredicate(predAtom);
					}
					else {
						LocalPredicateConjunct conj = new LocalPredicateConjunct();
						conj.addPredicate(currentPred);
						conj.addPredicate(predAtom);
						target.setPredicate(conj);
					}
				}
				else {
					// predicate is on aggregated function and needs to be filtered
					// build an optimizer predicate out of the parsed predicate
					LocalPredicateAtom predAtom = 
						new LocalPredicateAtom(pred, pc, literal);
					
					// make that predicate part of the table scans predicates
					if (havingPred == null) {
						havingPred = predAtom;
					}
					else if (havingPred instanceof LocalPredicateAtom) {
						LocalPredicateConjunct conj = new LocalPredicateConjunct();
						conj.addPredicate(havingPred);
						conj.addPredicate(predAtom);
						havingPred = conj;
					}
					else if (havingPred instanceof LocalPredicateConjunct) {
						((LocalPredicateConjunct) havingPred).addPredicate(predAtom);
					}
					else {
						throw new InternalOperationFailure("An unrecognized type of optimizer " +
								"predicate was found during the semantical checks", false, null);
					}
				}
			}
		}
		
		// -------------------------------------------------------------------------------
		//                                 Order By
		// -------------------------------------------------------------------------------

		
		if (query.getOrderByClause() != null) {
			int rank = 1;
			
			Iterator<OrderColumn> iter = query.getOrderByClause().getChildren();
			while (iter.hasNext()) {
				OrderColumn oc = iter.next();
				
				Integer index = colNamePosMap.get(
						oc.getColumn().getResultColumnName().toLowerCase(Constants.CASE_LOCALE));
				if (index == null) {
					throw new QuerySemanticsInvalidException("The column with output name '" + 
							oc.getColumn().getResultColumnName() +
							"', referenced in the ORBER BY clause, " +
							"is not defined in the SELECT clause.");
				}
				// check if column was already used for ORDER BY
				if(outputColumns[index].getOrderRank() == -1)
				{
					outputColumns[index].setOrderRank(rank++);
					if (oc.getOrder() == de.tuberlin.dima.minidb.parser.OrderColumn.Order.ASCENDING)
					{
						outputColumns[index].setOrder(Order.ASCENDING);
					} 
					else
					{
						outputColumns[index].setOrder(Order.DESCENDING);					
					}
				}
				else 
				{
					throw new QuerySemanticsInvalidException("The column '" + oc.getColumn().getResultColumnName() + "' appeared twice in ORDER BY.");
				}
			}
		}

		// -------------------------------------------------------------------------------
		//                    Pull up nested queries 
		// -------------------------------------------------------------------------------
		Collection<Relation> tableList = pullUpSubqueries(tables, joinEdges,
				cols2jointargets, outputColumns);
		
		// -------------------------------------------------------------------------------
		//                    Predicate post-processing and inference 
		// -------------------------------------------------------------------------------
		
		Relation[] tabs = tableList.toArray(new Relation[tables.size()]);		
		
		// check if the join predicates form a contiguous graph, since we do not support
		// Cartesian joins
		if (tabs.length > 1 && (joinEdges == null || !isGraphContiguous(tabs, joinEdges))) {
			throw new QuerySemanticsInvalidException(
					"The query does not specify joins between all involved tables. " +
					"That implies a cartesian join which is not supported right now.");
		}
		
		// TODO: check if algorithms depends on sorting
//		Arrays.sort(tabs);
		
		if (joinEdges != null && joinEdges.size() > 0) 
		{
			// sort the edges after the left table scan operator's id in ascending order
			Collections.sort(joinEdges, JOIN_EDGE_SORTER);	
			// first of all, we need to expand the join predicates, i.e. add implicit transitive join
			// predicates to give the optimizer more freedom to choose the join order
			expandJoinPredicates(joinEdges, cols2jointargets);
			// now, add implied local predicates.
			addImpliedLocalPredicates(tabs, cols2jointargets);
		}

		// finally, normalize all local predicates and make some inference about
		// their truth, i.e. whether they hold always or never.
		for (Iterator<Relation> iter = tableList.iterator(); iter.hasNext(); )
		{
			Relation rel = iter.next();
			LocalPredicate pred = rel.getPredicate();
			normalizeLocalPredicates(pred);
			// check if conjunction was reduced to a single predicate or no predicate
			if(pred instanceof LocalPredicateConjunct)
			{
				LocalPredicateConjunct conj = (LocalPredicateConjunct) pred;
				if(conj.getNumberOfPredicates() == 1)
				{
					rel.setPredicate(conj.getPredicates()[0]);
				}
				else if (conj.getNumberOfPredicates() == 0)
				{
					rel.setPredicate(null);
				}
			}
		}		
		
		// normalize the having predicate
		if (havingPred != null) {
			normalizeLocalPredicates(havingPred);
		}
		
		// build the analyzed query and pass it on
		return new Pair<AnalyzedSelectQuery, Integer>(new AnalyzedSelectQuery(tableList.toArray(new Relation[0]), joinEdges.toArray(new JoinGraphEdge[0]), outputColumns, havingPred, foundGrouped | foundAggregated), relationNumber);
	}


	/**
	 * Pulls up the nested queries into the direct join graph if possible.
	 * 
	 * @param tables The tables of the current select query.
	 * @param joinEdges The join edges of the current select query. 
	 * @param cols2jointargets The mapping of columns to relations of the current select query.
	 * @param outputColumns The output columns of the current select query.
	 * @return The updated list of relations of the current select query.
	 */
	protected Collection<Relation> pullUpSubqueries(
			Map<String, Relation> tables, List<JoinGraphEdge> joinEdges,
			Map<Long, List<TableOpAtomPair>> cols2jointargets,
			ProducedColumn[] outputColumns) 
	{
		// two pass algorithm needed, first mark then pull
		// first mark all relations that cannot be pulled up
		Collection<Relation> noPulling = markUnpullableSubqueries(tables, joinEdges,
				cols2jointargets, outputColumns);
		// after marking start pulling up
		// rewire output columns
		for(int i = 0; i < outputColumns.length; i++)
		{
			if(outputColumns[i].getRelation() instanceof AnalyzedSelectQuery && !noPulling.contains(outputColumns[i].getRelation()))
			{
				AnalyzedSelectQuery a = (AnalyzedSelectQuery) outputColumns[i].getRelation();
				// get output column of select query
				ProducedColumn pc = a.getOutputColumns()[outputColumns[i].getColumnIndex()];
				// replace output column with new output column linked to relation
				outputColumns[i] = new ProducedColumn(pc.getRelation(), outputColumns[i].getDataType(), pc.getColumnIndex(), outputColumns[i].getColumnAliasName(), pc.getParsedColumn());
			}
		}
		// rewire join edges
		for(int i = 0; i < joinEdges.size(); i++)
		{
			// rewire left node if necessary
			JoinGraphEdge jge = joinEdges.get(i);
			if (jge.getLeftNode() instanceof AnalyzedSelectQuery && !noPulling.contains(jge.getLeftNode()))
			{
				if(jge.getJoinPredicate() instanceof JoinPredicateAtom)
				{
					JoinPredicateAtom jpa = (JoinPredicateAtom) jge.getJoinPredicate();
					// generate new join edge referencing the table in the nested query
					JoinPredicateAtom jpaNew = pullUpLeftJoinPredicateAtom(
							cols2jointargets, jge, jpa);						
					// set new join predicate
					jge.setJoinPredicate(jpaNew);
					// set new left node of join edge
					jge.setLeftNode(jpaNew.getLeftHandOriginatingTable());
				} 
				else if(jge.getJoinPredicate() instanceof JoinPredicateConjunct)
				{
					JoinPredicateConjunct conj = (JoinPredicateConjunct) jge.getJoinPredicate();
					List<JoinPredicateAtom> conjList = conj.getConjunctiveFactors(); 
					for(int x = 0; x < conjList.size(); x++ )
					{
						JoinPredicateAtom jpa = conjList.get(x);
						// generate new join edge referencing the table in the nested query
						JoinPredicateAtom jpaNew = pullUpLeftJoinPredicateAtom(cols2jointargets, jge, jpa);
						conjList.set(x, jpaNew);
					}
				}
			}
			// rewire right node if necessary
			if (jge.getRightNode() instanceof AnalyzedSelectQuery && !noPulling.contains(jge.getRightNode()))
			{
				if(jge.getJoinPredicate() instanceof JoinPredicateAtom)
				{
					JoinPredicateAtom jpa = (JoinPredicateAtom) jge.getJoinPredicate();
					// generate new join edge referencing the table in the nested query
					JoinPredicateAtom jpaNew = pullUpRightJoinPredicateAtom(cols2jointargets, jge, jpa);
					jge.setJoinPredicate(jpaNew);
					jge.setRightNode(jpaNew.getRightHandOriginatingTable());
				} 
				else if(jge.getJoinPredicate() instanceof JoinPredicateConjunct)
				{
					JoinPredicateConjunct conj = (JoinPredicateConjunct) jge.getJoinPredicate();
					List<JoinPredicateAtom> conjList = conj.getConjunctiveFactors(); 
					for(int x = 0; x < conjList.size(); x++ )
					{
						JoinPredicateAtom jpa = conjList.get(x);
						// generate new join edge referencing the table in the nested query
						JoinPredicateAtom jpaNew = pullUpRightJoinPredicateAtom(cols2jointargets, jge, jpa);
						conjList.set(x, jpaNew);
					}
				}
			}
		}
		// rewire tables
		Iterator<Relation> it = tables.values().iterator();
		Collection<Relation> tableList = new LinkedList<Relation>();
		while(it.hasNext())
		{
			Relation rel = it.next();
			if(rel instanceof AnalyzedSelectQuery && !noPulling.contains(rel))
			{
				// get table accesses and merge them into new table collection
				for(int i = 0; i < ((AnalyzedSelectQuery) rel).getTableAccesses().length; i++)
				{
					tableList.add(((AnalyzedSelectQuery) rel).getTableAccesses()[i]);
				}
			}
			else
			{
				tableList.add(rel);
			}
		}
		return tableList;
	}


	/**
	 * Generates a new atomic join predicate on the left side and updates the references.
	 * 
	 * @param cols2jointargets The mapping of columns to relations of the current select query.
	 * @param jge The JoinGraphEdge associated with the atomic join predicate.
	 * @param jpa The old a tomic join predicate.
	 * @return The new atomic join predicate with updated references.
	 */
	protected JoinPredicateAtom pullUpLeftJoinPredicateAtom(
			Map<Long, List<TableOpAtomPair>> cols2jointargets,
			JoinGraphEdge jge, JoinPredicateAtom jpa) 
	{
		ProducedColumn pc = ((AnalyzedSelectQuery) jge.getLeftNode()).getOutputColumns()[jpa.getLeftHandColumn().getColumnIndex()];
		JoinPredicateAtom jpaNew = new JoinPredicateAtom(jpa.getParsedPredicate(), pc.getRelation(), jpa.getRightHandOriginatingTable(), pc, jpa.getRightHandColumn());
		
		Long lHash = (((long) jpa.getLeftHandOriginatingTable().hashCode()) << 32) | jpa.getLeftHandColumn().getColumnIndex();
		Long rHash = (((long) jpa.getRightHandOriginatingTable().hashCode()) << 32) | jpa.getRightHandColumn().getColumnIndex();
		List<TableOpAtomPair> oldLeftTarget = cols2jointargets.get(lHash);
		List<TableOpAtomPair> oldRightTarget = cols2jointargets.get(rHash);
		// remove old wiring
		cols2jointargets.remove(lHash);
		Long newHash = (((long) pc.getRelation().hashCode()) << 32) | pc.getColumnIndex();
		List<TableOpAtomPair> leftTarget = cols2jointargets.get(newHash);						
		List<TableOpAtomPair> rightTarget = new ArrayList<TableOpAtomPair>(); 						
		
		// add to the map as new targets 
		if (leftTarget == null) {
			leftTarget = new ArrayList<TableOpAtomPair>();
			cols2jointargets.put(newHash, leftTarget);
		}
		// add new pairs for new column
		for(int y = 0; y < oldLeftTarget.size(); y++)
		{
			if(oldLeftTarget.get(y).getPredicate() == jpa)
			{
				leftTarget.add(new TableOpAtomPair(jpa.getRightHandOriginatingTable(), jpaNew));
			} 
			else 
			{
				leftTarget.add(oldLeftTarget.get(y));
			}
		}
		// remove old pairs for column
		for(int y = 0; y < oldRightTarget.size(); y++)
		{
			if(oldRightTarget.get(y).getPredicate() == jpa)
			{
				rightTarget.add(new TableOpAtomPair(pc.getRelation(), jpaNew));						
			}
			else
			{
				rightTarget.add(oldRightTarget.get(y));								
			}
		}
		cols2jointargets.put(rHash, rightTarget);
		cols2jointargets.put(newHash, leftTarget);
		return jpaNew;
	}
	
	/**
	 * Generates a new atomic join predicate on the right side and updates the references.
	 * 
	 * @param cols2jointargets The mapping of columns to relations of the current select query.
	 * @param jge The JoinGraphEdge associated with the atomic join predicate.
	 * @param jpa The old a tomic join predicate.
	 * @return The new atomic join predicate with updated references.
	 */
	protected JoinPredicateAtom pullUpRightJoinPredicateAtom(
			Map<Long, List<TableOpAtomPair>> cols2jointargets,
			JoinGraphEdge jge, JoinPredicateAtom jpa) 
	{
		ProducedColumn pc = ((AnalyzedSelectQuery) jge.getRightNode()).getOutputColumns()[jpa.getRightHandColumn().getColumnIndex()];
		JoinPredicateAtom jpaNew = new JoinPredicateAtom(jpa.getParsedPredicate(), jpa.getLeftHandOriginatingTable(), pc.getRelation(), jpa.getLeftHandColumn(), pc);
		Long lHash = (((long) jpa.getLeftHandOriginatingTable().hashCode()) << 32) | jpa.getLeftHandColumn().getColumnIndex();
		Long rHash = (((long) jpa.getRightHandOriginatingTable().hashCode()) << 32) | jpa.getRightHandColumn().getColumnIndex();
		List<TableOpAtomPair> oldLeftTarget = cols2jointargets.get(lHash);
		List<TableOpAtomPair> oldRightTarget = cols2jointargets.get(rHash);
		// remove old wiring
		cols2jointargets.remove(rHash);
		Long newHash = (((long) pc.getRelation().hashCode()) << 32) | pc.getColumnIndex();
		List<TableOpAtomPair> leftTarget = new ArrayList<TableOpAtomPair>();
		List<TableOpAtomPair> rightTarget = cols2jointargets.get(newHash); 						
		
		// add to the map as new targets 
		if (rightTarget == null) {
			rightTarget = new ArrayList<TableOpAtomPair>();
			cols2jointargets.put(newHash, rightTarget);
		}
		// add new pairs for new column
		for(int y = 0; y < oldRightTarget.size(); y++)
		{
			if(oldRightTarget.get(y).getPredicate() == jpa)
			{
				rightTarget.add(new TableOpAtomPair(jpa.getLeftHandOriginatingTable(), jpaNew));
			} 
			else 
			{
				rightTarget.add(oldRightTarget.get(y));
			}
		}
		// remove old pairs for column
		for(int y = 0; y < oldLeftTarget.size(); y++)
		{
			if(oldLeftTarget.get(y).getPredicate() == jpa)
			{
				leftTarget.add(new TableOpAtomPair(pc.getRelation(), jpaNew));						
			}
			else
			{
				leftTarget.add(oldLeftTarget.get(y));								
			}
		}
		cols2jointargets.put(newHash, rightTarget);
		cols2jointargets.put(lHash, leftTarget);
		return jpaNew;
	}
	
	/**
	 * Marks all nested that cannot be pulled up due to defined restrictions.
	 * 
	 * @param tables The tables of the current select query.
	 * @param joinEdges The join edges of the current select query. 
	 * @param cols2jointargets The mapping of columns to relations of the current select query.
	 * @param outputColumns The output columns of the current select query.
	 * @return The collection of AnalyzedSelectQueries that cannot be pulled up.
	 */
	private Collection<Relation> markUnpullableSubqueries(Map<String, Relation> tables, List<JoinGraphEdge> joinEdges,
			Map<Long, List<TableOpAtomPair>> cols2jointargets,
			ProducedColumn[] outputColumns) 
	{
		Collection<Relation> noPulling = new HashSet<Relation>();	
		// check relations of output columns
		for(int i = 0; i < outputColumns.length; i++)
		{
			if(outputColumns[i].getRelation() instanceof AnalyzedSelectQuery && !((AnalyzedSelectQuery) outputColumns[i].getRelation()).isGrouping())
			{
				AnalyzedSelectQuery a = (AnalyzedSelectQuery) outputColumns[i].getRelation();
				// get output column of select query
				ProducedColumn pc = a.getOutputColumns()[outputColumns[i].getColumnIndex()];
				if(pc.getParsedColumn().getColumn() == pc.getParsedColumn().getExpression() &&
						outputColumns[i].getParsedColumn().getColumn() == outputColumns[i].getParsedColumn().getExpression())
				{
					// no restriction found
				} 
				else 
				{
					noPulling.add(outputColumns[i].getRelation());
				}
			} else {
				noPulling.add(outputColumns[i].getRelation());
			}
		}
		// check relations of join edges
		for(int i = 0; i < joinEdges.size(); i++)
		{
			JoinGraphEdge jge = joinEdges.get(i);
			// check left node and mark if necessary
			if (jge.getLeftNode() instanceof AnalyzedSelectQuery && !((AnalyzedSelectQuery) jge.getLeftNode()).isGrouping() && !noPulling.contains(jge.getLeftNode()))
			{
				if(jge.getJoinPredicate() instanceof JoinPredicateAtom)
				{
					JoinPredicateAtom jpa = (JoinPredicateAtom) jge.getJoinPredicate();
					ProducedColumn pc = ((AnalyzedSelectQuery) jge.getLeftNode()).getOutputColumns()[jpa.getLeftHandColumn().getColumnIndex()];
					// only pull-up if predicate and output column contain no expression
					if(pc.getParsedColumn().getColumn() == pc.getParsedColumn().getExpression() &&
							jpa.getParsedPredicate().getLeftHandColumn() == jpa.getParsedPredicate().getLeftHandExpression() &&
							jpa.getParsedPredicate().getRightHandColumn() == jpa.getParsedPredicate().getRightHandExpression())
					{
						// no restrictions found
					}
					else 
					{
						noPulling.add(jge.getLeftNode());
					}
				} else if(jge.getJoinPredicate() instanceof JoinPredicateConjunct)
				{
					JoinPredicateConjunct conj = (JoinPredicateConjunct) jge.getJoinPredicate();
					List<JoinPredicateAtom> conjList = conj.getConjunctiveFactors(); 
					for(int x = 0; x < conjList.size(); x++ )
					{
						JoinPredicateAtom jpa = conjList.get(x);
						ProducedColumn pc = ((AnalyzedSelectQuery) jge.getLeftNode()).getOutputColumns()[jpa.getLeftHandColumn().getColumnIndex()];
						// only pull-up if predicate and output column contain no expression
						if(pc.getParsedColumn().getColumn() == pc.getParsedColumn().getExpression() &&
								jpa.getParsedPredicate().getLeftHandColumn() == jpa.getParsedPredicate().getLeftHandExpression() &&
								jpa.getParsedPredicate().getRightHandColumn() == jpa.getParsedPredicate().getRightHandExpression())
						{
							// no restrictions found
						}					
						else 
						{
							noPulling.add(jge.getLeftNode());
						}
					}
				}
			}
			else 
			{
				noPulling.add(jge.getLeftNode());
			}
			// check right node and mark if necessary
			if (jge.getRightNode() instanceof AnalyzedSelectQuery && !((AnalyzedSelectQuery) jge.getRightNode()).isGrouping() && !noPulling.contains(jge.getRightNode()))
			{
				if(jge.getJoinPredicate() instanceof JoinPredicateAtom)
				{
					JoinPredicateAtom jpa = (JoinPredicateAtom) jge.getJoinPredicate();
					ProducedColumn pc = ((AnalyzedSelectQuery) jge.getRightNode()).getOutputColumns()[jpa.getRightHandColumn().getColumnIndex()];
					// only pull-up if predicate and output column contain no expression
					if(pc.getParsedColumn().getColumn() == pc.getParsedColumn().getExpression() &&
							jpa.getParsedPredicate().getLeftHandColumn() == jpa.getParsedPredicate().getLeftHandExpression() &&
							jpa.getParsedPredicate().getRightHandColumn() == jpa.getParsedPredicate().getRightHandExpression())
					{
						// no restrictions found
					}			
					else 
					{
						noPulling.add(jge.getRightNode());
					}
				} else if(jge.getJoinPredicate() instanceof JoinPredicateConjunct)
				{
					JoinPredicateConjunct conj = (JoinPredicateConjunct) jge.getJoinPredicate();
					List<JoinPredicateAtom> conjList = conj.getConjunctiveFactors(); 
					for(int x = 0; x < conjList.size(); x++ )
					{
						JoinPredicateAtom jpa = conjList.get(x);
						ProducedColumn pc = ((AnalyzedSelectQuery) jge.getLeftNode()).getOutputColumns()[jpa.getLeftHandColumn().getColumnIndex()];
						// only pull-up if predicate and output column contain no expression
						if(pc.getParsedColumn().getColumn() == pc.getParsedColumn().getExpression() &&
								jpa.getParsedPredicate().getLeftHandColumn() == jpa.getParsedPredicate().getLeftHandExpression() &&
								jpa.getParsedPredicate().getRightHandColumn() == jpa.getParsedPredicate().getRightHandExpression())
						{
							// no restrictions found
						}
						else 
						{
							noPulling.add(jge.getRightNode());
						}
					}
				}
			}
			else 
			{
				noPulling.add(jge.getRightNode());
			}
		}
		
		return noPulling;
	}


	/**
	 * Adds transitive join predicates to the list.
	 * The algorithm uses the following principals:
	 * <ul>
	 *   <li>The left scan's id on an edge is always lower than the right scan's.</li>
	 *   <li>The edges are in a sorted order, primarily after the left table, secondarily
	 *       after the right table.</li>
	 *   <li>Transitive edges are always added from left to right.</li>
	 * </ul>
	 * The following steps are executed:
	 * <ul>
	 *   <li>Go over the edges and for each edge take the left table's known targets.</li>
	 *   <li>Take the right table from the edge and add transitive edges from it to the
	 *       known targets as before, but only, if the target has a greater id.</li>
	 *   <li>Add the transitive edge to the known targets of the right table and target table.</li>
	 *   <li>Repeat the same with right and left table switched.</li>
	 * </ul>
	 * 
	 * A prerequisite for this function is that the list of join edges is sorted after the left (and right)
	 * table scan operators is in ascending order.
	 * 
	 * @param joinEdges The list of join predicates.
	 * @param cols2Targets A map from a hash over the columns to the table operators with
	 *                     the join predicate that they join to.
	 */
	private void expandJoinPredicates(List<JoinGraphEdge> joinEdges,
			Map<Long, List<TableOpAtomPair>> cols2targets)
	{
		// list of transitively added edges
		List<JoinGraphEdge> newEdges = new ArrayList<JoinGraphEdge>();
		
		// the incrementing predicate ID.
//		int predID = 1;
		
		// go over all join edges
		for (int i = 0; i < joinEdges.size(); i++)
		{			
			JoinGraphEdge edge = joinEdges.get(i);
			JoinPredicate joinPred = edge.getJoinPredicate();
			
			if (joinPred instanceof JoinPredicateAtom) {
				// only one predicate
				JoinPredicateAtom atom = (JoinPredicateAtom) joinPred;
				if (atom.getParsedPredicate().getOp() == Predicate.Operator.EQUAL) {
					// only equality join predicates are expanded here
					// if they have not yet been processed
//					if (atom.getPredID() == -1) {
//						atom.setPredID(predID++);
//					}
					expandJoinPredicateAtom(atom, edge, cols2targets, newEdges);
				}
			}
			else if (joinPred instanceof JoinPredicateConjunct) {
				// list of predicates
				List<JoinPredicateAtom> factors = 
					((JoinPredicateConjunct) joinPred).getConjunctiveFactors();
				for (int f = 0; f < factors.size(); f++) {
					// each atom is expanded
					JoinPredicateAtom atom = factors.get(f);
					if (atom.getParsedPredicate().getOp() == Predicate.Operator.EQUAL)
					{
						// only equality join predicates are expanded here
//						if (atom.getPredID() == -1) {
//							atom.setPredID(predID++);
//						}
						expandJoinPredicateAtom(atom, edge, cols2targets, newEdges);
					}
				}
			}
		}
		
		// the new join edges may contain edges, where we have edges between the tables,
		// but for a different join predicate. We must now merge those.
		if (!newEdges.isEmpty()) {
			Collections.sort(newEdges, JOIN_EDGE_SORTER);
		
			int newIndex = 0;
			for (JoinGraphEdge edge : joinEdges) {
				
				for (; newIndex < newEdges.size(); newIndex++) {
					JoinGraphEdge currentNew = newEdges.get(newIndex);
					int cmp = JOIN_EDGE_SORTER.compare(edge, currentNew);
					
					// decide upon comparison
					if (cmp < 0) {
						// the old edge is still smaller, so we break and let the
						// outer loop do some more iterations
						break;
					}
					else if (cmp > 0) {
						// we are still smaller, do some more iterations
						continue;
					}
					else {
						// same set of tables, merge now
						JoinPredicate pred = edge.getJoinPredicate();
						if (pred instanceof JoinPredicateAtom) {
							JoinPredicateConjunct conj = new JoinPredicateConjunct();
							conj.addJoinPredicate(pred);
							conj.addJoinPredicate(currentNew.getJoinPredicate());
							edge.setJoinPredicate(conj);
						}
						else if (pred instanceof JoinPredicateConjunct) {
							// already contained a conjunct, add the new predicate
							JoinPredicateConjunct conj = 
								(JoinPredicateConjunct) pred;
							conj.addJoinPredicate(currentNew.getJoinPredicate());
						}
						else {
							throw new InternalOperationFailure(
								"Optimizer encountererd a bug during join predicate expansion.",
								false, null);
						}
						
						// erase it from the old list
						newEdges.set(newIndex, null);
					}
				}
			}
				
			// add the non-null edges
			boolean more = false;
			for (JoinGraphEdge e : newEdges) {
				if (e != null) {
					joinEdges.add(e);
					more = true;
				}
			}
			if (more) {
				Collections.sort(joinEdges, JOIN_EDGE_SORTER);
			}
		}
	}
	
	
	/**
	 * Checks if for a given join edge and a given <tt>JoinPredicateAtom</tt>, there
	 * are transitive join predicates to be added.
	 * 
	 * @param atom The join predicate atom to expand.
	 * @param edge The edge from which the atom is taken, possibly as part of a conjunct.
	 * @param cols2targets The maps from columns (in scope of tables) to the targets they are known
	 *                     to join to, also transitively.
	 * @param newEdges The list collecting the new targets that were added.
	 */
	private final void expandJoinPredicateAtom(JoinPredicateAtom atom, JoinGraphEdge edge,
			Map<Long, List<TableOpAtomPair>> cols2targets, List<JoinGraphEdge> newEdges)
	{
		Relation leftTable = edge.getLeftNode();
		Relation rightTable = edge.getRightNode();
		
		List<TableOpAtomPair> knownTargetsForLeft = cols2targets.get(
				(((long) leftTable.hashCode()) << 32) | atom.getLeftHandColumn().getColumnIndex());
		
		List<TableOpAtomPair> knownTargetsForRight = cols2targets.get(
				(((long) rightTable.hashCode()) << 32) | atom.getRightHandColumn().getColumnIndex());
		
		// add transitive edges from the right node to all known targets of the
		// left node that have a greater id
		for (int i = 0; i < knownTargetsForLeft.size(); i++) {
			TableOpAtomPair currentPair = knownTargetsForLeft.get(i);
			Relation currentPop = currentPair.getPop();
			JoinPredicateAtom currentAtom = currentPair.getPredicate();
			
			// if the id of the target is lower, we can skip
			if (currentPop.getID() <= rightTable.getID()) {
				continue;
			}
			
			// now check if the current table is already known as a target of the right
			// if yes, then there is already an edge and we need not add another one
			boolean known = false;
			for (int k = 0; k < knownTargetsForRight.size(); k++) {
				TableOpAtomPair rightTarget = knownTargetsForRight.get(k);
				if (rightTarget.getPop() == currentPop) {
					// it is already known. if this has not been added
					// transitively earlier, mark it with the same ID
					// to indicate it is transitively connected
//					if (rightTarget.getPredicate().getPredID() == -1) {
//						rightTarget.getPredicate().setPredID(atom.getPredID());
//					}
					known = true;
					break;
				}
			}
			if (known) {
				continue;
			}
			
			// also mark the current edge with the same ID as it is transitively
			// connected to the current atom
//			currentAtom.setPredID(atom.getPredID());
						
			// transitive edge to be added where the new edge's left node will be the current right
			// and the new edge's right node will be the current target
			Predicate pp = new Predicate();
			pp.setOperator(Predicate.Operator.EQUAL);
			pp.setLeftHandSide(atom.getParsedPredicate().getRightHandExpression(), atom.getParsedPredicate().getRightHandColumn());
			pp.setRightHandSide(currentAtom.getParsedPredicate().getRightHandExpression(), currentAtom.getParsedPredicate().getRightHandColumn());
			
			JoinPredicateAtom newAtom = new JoinPredicateAtom(
					pp, rightTable, currentPop,
					atom.getRightHandColumn(),
					currentAtom.getRightHandColumn());
//			newAtom.setPredID(atom.getPredID());
			
			// add the new transitive edge
			newEdges.add(new JoinGraphEdge(rightTable, currentPop, newAtom));
			
			// add to the right table's list of known targets
			knownTargetsForRight.add(new TableOpAtomPair(currentPop, newAtom));
			
			// add to the current pop's list of known targets
			List<TableOpAtomPair> popList = cols2targets.get(
					(((long) currentPop.hashCode()) << 32) | currentAtom.getLeftHandColumn().getColumnIndex());
			if(popList == null)
			{
				// this should not be the case?! // TODO:
				popList = new ArrayList<TableOpAtomPair>();
				cols2targets.put((((long) currentPop.hashCode()) << 32) | currentAtom.getLeftHandColumn().getColumnIndex(), popList);
			}
			popList.add(new TableOpAtomPair(rightTable, newAtom));
		}
		
		// now add transitive edges from the left node to all known targets of
		// the right node
		for (int i = 0; i < knownTargetsForRight.size(); i++) {
			TableOpAtomPair currentPair = knownTargetsForRight.get(i);
			Relation currentPop = currentPair.getPop();
			JoinPredicateAtom currentAtom = currentPair.getPredicate();
			
			// if the id of the target is lower, we can skip
			if (currentPop.getID() <= leftTable.getID()) {
				continue;
			}
			
			// now check if the current table is already known as a target of the left table
			// if yes, then there is already an edge and we need not add another one
			boolean known = false;
			for (int k = 0; k < knownTargetsForLeft.size(); k++) {
				TableOpAtomPair leftTarget = knownTargetsForLeft.get(k);
				if (leftTarget.getPop() == currentPop) {
					// it is already known. if this has not been added
					// transitively earlier, mark it with the same ID
					// to indicate it is transitively connected
//					if (leftTarget.getPredicate().getPredID() == -1) {
//						leftTarget.getPredicate().setPredID(atom.getPredID());
//					}
					known = true;
					break;
				}
			}
			if (known) {
				continue;
			}
			
			// also mark the current edge with the same ID as it is transitively
			// connected to the current atom
//			currentAtom.setPredID(atom.getPredID());
			
			Column currentPopsColumn = null;
			de.tuberlin.dima.minidb.parser.Column currentPopsCol = null;
			ParseTreeNode currentPopsExpr = null;
			if (currentPop.getID() < rightTable.getID()) {
				currentPopsColumn = currentAtom.getLeftHandColumn();
				currentPopsCol = currentAtom.getParsedPredicate().getLeftHandColumn();
				currentPopsExpr = currentAtom.getParsedPredicate().getLeftHandExpression();
			}
			else {
				currentPopsColumn = currentAtom.getRightHandColumn();
				currentPopsCol = currentAtom.getParsedPredicate().getRightHandColumn();
				currentPopsExpr = currentAtom.getParsedPredicate().getRightHandExpression();
			}
			
			// transitive edge to be added where the new edge'sleft node will be the current right
			// and the new edge's right node will be the current target
			Predicate pp = new Predicate();
			pp.setOperator(Predicate.Operator.EQUAL);
			pp.setLeftHandSide(atom.getParsedPredicate().getLeftHandExpression(), atom.getParsedPredicate().getLeftHandColumn());
			pp.setRightHandSide(currentPopsExpr, currentPopsCol);
			
			JoinPredicateAtom newAtom = new JoinPredicateAtom(
					pp, leftTable, currentPop,
					atom.getLeftHandColumn(),
					currentPopsColumn);
//			newAtom.setPredID(atom.getPredID());
			
			// add the new transitive edge
			newEdges.add(new JoinGraphEdge(leftTable, currentPop, newAtom));
			
			// add to the left table's list of known targets
			knownTargetsForLeft.add(new TableOpAtomPair(currentPop, newAtom));
			
			// add to the current pop's list of known targets
			List<TableOpAtomPair> popList = cols2targets.get(
					(((long) currentPop.hashCode()) << 32) | currentPopsColumn.getColumnIndex());
			popList.add(new TableOpAtomPair(leftTable, newAtom));
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                            Local Predicate Normalization
	// --------------------------------------------------------------------------------------------

	
	/**
	 * Adds the local predicates that are implied by the combination of join predicates and
	 * other local predicates. For example, if a query has a WHERE clause like
	 * <pre>WHERE t1.pk &gt; 3 AND t1.pk = t2.fk</pre> than that implies another local
	 * predicate: <pre>t2.fk &gt; 3</pre>
	 * 
	 * Prerequisite: The array must be sorted after the scanId. No between must be contained, yet.
	 *               This should be okay, as between-predicates is a result of normalization, which
	 *               follows this step. 
	 * 
	 * @param tabs The table scan operators representing the accesses to local tables.
	 * @param cols2jointargets The map from join column hash to the targets. 
	 * @throws QuerySemanticsInvalidException Thrown if a DataFormatException occurs.
	 *  
	 */
	private void addImpliedLocalPredicates(Relation[] tabs, Map<Long, List<TableOpAtomPair>> cols2jointargets) throws QuerySemanticsInvalidException
	{
		Map<Integer, List<LocalPredicateAtom>> newPreds =
			new HashMap<Integer, List<LocalPredicateAtom>>();
		// go over all table scans and find local predicate atoms or atoms in top level conjunctions
		for (Relation tab : tabs) 
		{
			LocalPredicate pred = tab.getPredicate();			
			if (pred != null) 
			{
				// top level atom
				if (pred instanceof LocalPredicateAtom)
				{
					LocalPredicateAtom atom = (LocalPredicateAtom) pred;
					// find the targets for this column as a join column
					List<TableOpAtomPair> targets = cols2jointargets.get(
							(((long) tab.hashCode()) << 32) | atom.getColumn().getColumnIndex());
					if (targets != null) 
					{
						addImpliedPredicate(atom, targets, newPreds);
					}
				}
				else if (pred instanceof LocalPredicateConjunct) 
				{
					LocalPredicateConjunct conj = (LocalPredicateConjunct) pred;
					for (LocalPredicate p : conj.getPredicates()) 
					{
						if (p instanceof LocalPredicateAtom) 
						{
							// atom in top level conjunction, this will be transitively added
							// because join predicates are also in the top level conjunction
							// further nested predicate will not be propagated
							LocalPredicateAtom atom = (LocalPredicateAtom) p;
							// find the targets for this column as a join column
							List<TableOpAtomPair> targets = cols2jointargets.get(
									(((long) tab.hashCode()) << 32) | atom.getColumn().getColumnIndex());
							if (targets != null) 
							{
								addImpliedPredicate(atom, targets, newPreds);
							}
						}
					}
				}
			}
		}
		
		// we went over all tables, not add the new predicates to the tables
		for (Map.Entry<Integer, List<LocalPredicateAtom>> entry : newPreds.entrySet()) {
			int index = entry.getKey().intValue();
			if (index >= 0 && index < tabs.length) {
				// do some sanity checks that upon a programming error loose some
				// opportunity, but do not crash
				Relation target = tabs[index];
				if (target.getID() != entry.getKey().intValue()) {
					continue;
				}
				
				LocalPredicate targetPred = target.getPredicate();
				
				if (targetPred == null) 
				{
					// the table had no predicate, yet
					List<LocalPredicateAtom> atoms = entry.getValue();
					if (atoms.size() == 1) 
					{
						target.setPredicate(atoms.get(0));
					}
					else if (atoms.size() > 1) 
					{
						LocalPredicateConjunct conj = new LocalPredicateConjunct();
						for (LocalPredicateAtom a : atoms) 
						{
							conj.addPredicate(a);
						}
						target.setPredicate(conj);
					}
				}
				else if (targetPred instanceof LocalPredicateConjunct) 
				{
					// the table had already a conjunct of predicates
					LocalPredicateConjunct conj = (LocalPredicateConjunct) targetPred;
					for (LocalPredicateAtom a : entry.getValue()) 
					{
						conj.addPredicate(a);
					}
				}
				else 
				{
					// some other predicate was present, make a conjunction with this one out of it
					LocalPredicateConjunct conj = new LocalPredicateConjunct();
					for (LocalPredicateAtom a : entry.getValue()) 
					{
						conj.addPredicate(a);
					}
					conj.addPredicate(targetPred);
					target.setPredicate(conj);
				}
			}
		}
	}


	/**
	 * Adds the predicates implied by a single predicate.
	 * 
	 * @param pred The predicate atom to add to other edges.
	 * @param cols2jointargets The 
	 * @throws QuerySemanticsInvalidException Thrown, if a DataException occurs.
	 */
	private void addImpliedPredicate(LocalPredicateAtom pred, List<TableOpAtomPair> targets, Map<Integer, List<LocalPredicateAtom>> newPreds) throws QuerySemanticsInvalidException
	{		
		// check that no expression is used in predicate
		if(pred.getParsedPredicate().getLeftHandColumn() == pred.getParsedPredicate().getLeftHandExpression() &&
				pred.getParsedPredicate().getRightHandLiteral() == pred.getParsedPredicate().getRightHandExpression())
		{
			for (TableOpAtomPair target : targets) 
			{			
				JoinPredicateAtom connectPredicate = target.getPredicate();
				LocalPredicateAtom newAtom = null;
				
				// only add implied predicates if no expressions are involved
				if (!(connectPredicate.getParsedPredicate().getLeftHandColumn() == connectPredicate.getParsedPredicate().getLeftHandExpression()
						&& connectPredicate.getParsedPredicate().getRightHandColumn() == connectPredicate.getParsedPredicate().getRightHandExpression()))
				{
					continue;
				}
				// check if predicates share an endpoint
				if (connectPredicate.getRightHandOriginatingTable() == target.getPop()) 
				{
					// the target table (to add the local predicate to) is the right hand side of
					// the join predicate				
					Predicate pp = new Predicate();
					pp.setLeftHandSide(connectPredicate.getParsedPredicate().getRightHandExpression(), connectPredicate.getParsedPredicate().getRightHandColumn());
					pp.setOperator(pred.getParsedPredicate().getOp());
					pp.setRightHandSide(pred.getParsedPredicate().getRightHandLiteral());
					newAtom = new LocalPredicateAtom(pp, connectPredicate.getRightHandColumn(), pred.getLiteral());
				}
				else if (connectPredicate.getLeftHandOriginatingTable() == target.getPop()) {
					// the target table (to add the local predicate to) is the left hand side of
					// the join predicate
					Predicate pp = new Predicate();
					pp.setLeftHandSide(connectPredicate.getParsedPredicate().getLeftHandExpression(), connectPredicate.getParsedPredicate().getLeftHandColumn());
					pp.setOperator(pred.getParsedPredicate().getOp());
					pp.setRightHandSide(pred.getParsedPredicate().getRightHandLiteral());
					newAtom = new LocalPredicateAtom(pp, connectPredicate.getLeftHandColumn(), pred.getLiteral());
				}
				else {
					continue;
				}
				Integer id = new Integer(target.getPop().getID());
				List<LocalPredicateAtom> atoms = newPreds.get(id);
				if (atoms == null) {
					atoms = new ArrayList<LocalPredicateAtom>();
					newPreds.put(id, atoms);
				}
				atoms.add(newAtom);
			}
		}
	}


	/**
	 * Normalizes the given local predicates. This method throws away redundant predicates
	 * and determines whether the predicates are always true or false.
	 * 
	 * @param predicate The predicate to normalize.
	 * @throws QuerySemanticsInvalidException Thrown if a DataException occurs.
	 */
	private void normalizeLocalPredicates(LocalPredicate predicate) throws QuerySemanticsInvalidException
	{
		if (predicate == null || predicate instanceof LocalPredicateAtom ||
				predicate instanceof LocalPredicateBetween)
		{
			return; // nothing to do
		}
		else if (predicate instanceof LocalPredicateConjunct) {
			LocalPredicateConjunct conj = (LocalPredicateConjunct) predicate;
			PredicateTruth pt = normalizeLocalPredicateConjunction(conj);
			predicate.setTruth(pt);
			if (pt == PredicateTruth.ALWAYS_TRUE) {
				conj.clearPredicates();
			}
			if(pt == PredicateTruth.ALWAYS_FALSE)
			{
				throw new QuerySemanticsInvalidException("The predicate conjunction '" + conj.toString() + "' always evaluates to false.");
			}
		}
		else {
			throw new IllegalArgumentException(
					"An unknown predicate type was submitted for normalization");
		}
	}
	
	
	/**
	 * Normalizes the given predicate conjunction. This method throws away redundant predicates
	 * and determines whether the conjunction is always false.
	 *  
	 * @param conjunct The conjunction to normalize.
	 * @return The truth of the predicate.
	 * @throws QuerySemanticsInvalidException 
	 */
	private PredicateTruth normalizeLocalPredicateConjunction(LocalPredicateConjunct conjunct) throws QuerySemanticsInvalidException
	{
		LocalPredicate[] factors = conjunct.getPredicates();
		if (factors.length == 0) {
			// no predicates to fulfill initially, so we are true
			return PredicateTruth.ALWAYS_TRUE;
		}
		
		List<LocalPredicate> preds = new ArrayList<LocalPredicate>();
		
		// get the individual factors and normalize them
		for (int i = 0; i < factors.length; i++) {
			LocalPredicate p = factors[i];
			if (p instanceof LocalPredicateAtom || 
					p instanceof LocalPredicateBetween)
			{
				// atoms and between are always normalized
				preds.add(p);
			}
			else if (p instanceof LocalPredicateConjunct) {
				PredicateTruth pt = normalizeLocalPredicateConjunction(
						(LocalPredicateConjunct) p);
				if (pt == PredicateTruth.ALWAYS_FALSE) {
					// a factor is false, so the whole conjunction if false
					return PredicateTruth.ALWAYS_FALSE;
				}
				else if (pt == PredicateTruth.ALWAYS_TRUE) {
					// skip this factor
				}
				else {
					// add this factor's factors
					LocalPredicate[] fs = 
						((LocalPredicateConjunct) p).getPredicates();
					for (int j = 0; j < fs.length; j++) {
						preds.add(fs[j]);
					}
				}
				
			}
		}
		
		// bring the atoms to the front
		Collections.sort(preds, PREDICATE_SORTER);
		
		Column currentColumn = null;
		LocalPredicateAtom lastEquality = null;
		int unequalStart = -1, unequalStop = -1;
		int lastUpperBoundIndex = -1;
		int lastLowerBoundIndex = -1;

		
		// go over all atoms
		int i = 0;
		for (; i < preds.size(); i++)
		{
			LocalPredicate lp = preds.get(i);
			if (!(lp instanceof LocalPredicateAtom)) {
				break;
			}
			
			// get the current atom with properties
			LocalPredicateAtom atom = (LocalPredicateAtom) lp;
			Column col = atom.getColumn();
			Predicate.Operator op = atom.getParsedPredicate().getOp();
			
			if (currentColumn == null || !currentColumn.equals(col)) {
				// new column
				// check if we have an upper and lower bound on the previous column
				// and create a between predicate
				if (lastLowerBoundIndex != -1 && lastUpperBoundIndex != -1) {
					LocalPredicateAtom lb = 
						(LocalPredicateAtom) preds.get(lastLowerBoundIndex);
					LocalPredicateAtom ub = 
						(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
					LocalPredicateBetween between = new LocalPredicateBetween(currentColumn,
							lb.getParsedPredicate(), ub.getParsedPredicate(),
							lb.getLiteral(), ub.getLiteral());
					
					preds.set(lastLowerBoundIndex, between);
					preds.set(lastUpperBoundIndex, null);
				}
				
				currentColumn = col;
				lastEquality = null;
				unequalStart = unequalStop = -1;
				lastUpperBoundIndex = -1;
				lastLowerBoundIndex = -1;
			}

			// same column
			if (op == Predicate.Operator.EQUAL) {
				// equal is first in sort order.
				// compare only to other equals
				
				if (lastEquality == null) {
					lastEquality = atom;
				}
				else {
					// two different equalities on the same column.
					// if they have the same literal, drop one
					// if they have different ones, mask the conjunction as always false
					if (lastEquality.getLiteral().equals(atom.getLiteral())) {
						preds.set(i, null);
					}
					else {
						return PredicateTruth.ALWAYS_FALSE;
					}
				}
			}
			else if (op == Predicate.Operator.NOT_EQUAL) {
				if (lastEquality != null) {
					// equality and inequality on same column
					// if literal is same, conjunction is always false
					// else inequality is redundant
					if (lastEquality.getLiteral().equals(atom.getLiteral())) {
						return PredicateTruth.ALWAYS_FALSE;
					}
					else {
						// same, we can drop the current
						preds.set(i, null);
					}
				}
				else {
					if (unequalStart == -1) {
						unequalStart = unequalStop = i;
					}
					else {
						for (int k = unequalStart; k <= unequalStop; k++) {
							LocalPredicateAtom p = (LocalPredicateAtom) preds.get(k);
							if (p == null) {
								continue;
							}
							if (p.getLiteral().equals(atom.getLiteral())) {
								// drop this one, it is redundant
								preds.set(i, null);
							}
						}
						unequalStop = i;
					}
				}
			}
			else if (op == Predicate.Operator.SMALLER) {
				// for smaller, check if this contradicts the equality
				if (lastEquality != null) {
					int cmp = atom.getLiteral().compareTo(lastEquality.getLiteral());
					if (cmp > 0) {
						// range contains equality, range is redundant
						preds.set(i, null);
					}
					else {
						// range and equality contradict
						return PredicateTruth.ALWAYS_FALSE;
					}
				}
				else {
					// check if it makes inequalities redundant
					if (unequalStart != -1) {
						for (int k = unequalStart; k <= unequalStop; k++) {
							LocalPredicateAtom p = (LocalPredicateAtom) preds.get(k);
							if (p == null) {
								continue;
							}
							
							int cmp = atom.getLiteral().compareTo(p.getLiteral());
							if (cmp <= 0) {
								preds.set(k, null);
							}
						}
					}
					
					// check against other upper bounds
					if (lastUpperBoundIndex != -1) {
						LocalPredicateAtom lastUpperBound = 
							(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
						// either we are the same, or the higher one is redundant
						int cmp = atom.getLiteral().compareTo(lastUpperBound.getLiteral());
						if (cmp < 0) {
							// this range predicate is more restrictive than the other one
							preds.set(lastUpperBoundIndex, null);
							lastUpperBoundIndex = i;
						}
						else {
							// this one is equal or less restrictive, drop it
							preds.set(i, null);
						}
					}
					else {
						lastUpperBoundIndex = i;
					}
				}
			}
			else if (op == Predicate.Operator.SMALLER_OR_EQUAL) {
				// check if this contradicts the equality
				if (lastEquality != null) {
					int cmp = atom.getLiteral().compareTo(lastEquality.getLiteral());
					if (cmp >= 0) {
						// range contains equality, range is redundant
						preds.set(i, null);
					}
					else {
						// range and equality contradict
						return PredicateTruth.ALWAYS_FALSE;
					}
				}
				else {
					// check if it makes inequalities redundant
					if (unequalStart != -1) {
						for (int k = unequalStart; k <= unequalStop; k++) {
							LocalPredicateAtom p = (LocalPredicateAtom) preds.get(k);
							if (p == null) {
								continue;
							}
							
							int cmp = atom.getLiteral().compareTo(p.getLiteral());
							if (cmp < 0) {
								// inequality outside the range
								preds.set(k, null);
							}
							else if (cmp == 0) {
								// smaller-equal + not-equal = smaller
								preds.set(k, null);
								
								Predicate pp = new Predicate();
								pp.setLeftHandSide(atom.getParsedPredicate().getLeftHandExpression(), atom.getParsedPredicate().getLeftHandColumn());
								pp.setRightHandSide(atom.getParsedPredicate().getRightHandLiteral());
								pp.setOperator(Predicate.Operator.SMALLER);
								op = Predicate.Operator.SMALLER;
								atom = new LocalPredicateAtom(pp, col, atom.getLiteral());
								preds.set(i, atom);
							}
						}
					}
					
					// compare against other ranges
					if (lastUpperBoundIndex != -1) {
						LocalPredicateAtom lastUpperBound = 
							(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
						// either we are the same, or the higher one is redundant
						int cmp = atom.getLiteral().compareTo(lastUpperBound.getLiteral());
						if (cmp < 0) {
							// we are smaller, replace the last upper bound
							preds.set(lastUpperBoundIndex, null);
							lastUpperBoundIndex = i;
						}
						else {
							// we are larger and hence redundant
							preds.set(i, null);
						}
					}
					else {
						lastUpperBoundIndex = i;
					}
				}
			}
			else if (op == Predicate.Operator.GREATER) {
				// for greater, check if this contradicts the equality
				if (lastEquality != null) {
					int cmp = atom.getLiteral().compareTo(lastEquality.getLiteral());
					if (cmp < 0) {
						// range contains equality, range is redundant
						preds.set(i, null);
					}
					else {
						// range and equality contradict
						return PredicateTruth.ALWAYS_FALSE;
					}
				}
				else {
					// check if it makes inequalities redundant
					if (unequalStart != -1) {
						for (int k = unequalStart; k <= unequalStop; k++) {
							LocalPredicateAtom p = (LocalPredicateAtom) preds.get(k);
							if (p == null) {
								continue;
							}
							
							int cmp = atom.getLiteral().compareTo(p.getLiteral());
							if (cmp >= 0) {
								preds.set(k, null);
							}
						}
					}
					
					// check if it contradicts the upper bounds
					if (lastUpperBoundIndex != -1) {
						LocalPredicateAtom lastUpperBound = 
							(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
						int cmp = atom.getLiteral().compareTo(lastUpperBound.getLiteral());
						if (cmp >= 0) {
							// this predicate contradicts the upper bound predicate
							return PredicateTruth.ALWAYS_FALSE;
						}
					}
					
					// check against other lower bounds
					if (lastLowerBoundIndex != -1) {
						LocalPredicateAtom lastLowerBound = 
							(LocalPredicateAtom) preds.get(lastLowerBoundIndex);
						// either we are the same, or the lower one is redundant
						int cmp = atom.getLiteral().compareTo(lastLowerBound.getLiteral());
						if (cmp > 0) {
							// this range predicate is more restrictive than the other one
							preds.set(lastLowerBoundIndex, null);
							lastLowerBoundIndex = i;
						}
						else {
							// this one is equal or less restrictive, drop it
							preds.set(i, null);
						}
					}
					else {
						lastLowerBoundIndex = i;
					}
				}
			}
			else if (op == Predicate.Operator.GREATER_OR_EQUAL) {
				// check if this contradicts the equality
				if (lastEquality != null) {
					int cmp = atom.getLiteral().compareTo(lastEquality.getLiteral());
					if (cmp <= 0) {
						// range contains equality, range is redundant
						preds.set(i, null);
					}
					else {
						// range and equality contradict
						return PredicateTruth.ALWAYS_FALSE;
					}
				}
				else {
					// check if it makes inequalities redundant
					if (unequalStart != -1) {
						for (int k = unequalStart; k <= unequalStop; k++) {
							LocalPredicateAtom p = (LocalPredicateAtom) preds.get(k);
							if (p == null) {
								continue;
							}
							
							int cmp = atom.getLiteral().compareTo(p.getLiteral());
							if (cmp > 0) {
								// inequality outside the range
								preds.set(k, null);
							}
							else if (cmp == 0) {
								// greater-equal + not-equal = greater
								preds.set(k, null);
								
								Predicate pp = new Predicate();
								pp.setLeftHandSide(atom.getParsedPredicate().getLeftHandExpression(), atom.getParsedPredicate().getLeftHandColumn());
								pp.setRightHandSide(atom.getParsedPredicate().getRightHandLiteral());
								pp.setOperator(Predicate.Operator.GREATER);
								op = Predicate.Operator.GREATER;
								atom = new LocalPredicateAtom(pp, col, atom.getLiteral());
								preds.set(i, atom);
							}
						}
					}
					
					// check if it contradicts the upper bounds
					if (lastUpperBoundIndex != -1) {
						LocalPredicateAtom lastUpperBound = 
							(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
						int cmp = atom.getLiteral().compareTo(lastUpperBound.getLiteral());
						if (cmp > 0) {
							// this predicate contradicts the upper bound predicate
							return PredicateTruth.ALWAYS_FALSE;
						}
						else if (cmp == 0) {
							// check if the upper bound includes the bound
							if (lastUpperBound.getParsedPredicate().getOp() == 
								Predicate.Operator.SMALLER_OR_EQUAL)
							{
								// bound is included, this becomes an equality predicate
								if (lastLowerBoundIndex != -1) {
									preds.set(lastLowerBoundIndex, null);
									lastLowerBoundIndex = -1;
								}
								
								preds.set(lastUpperBoundIndex, null);
								lastUpperBoundIndex = -1;
								
								Predicate pp = new Predicate();
								pp.setLeftHandSide(lastUpperBound.getParsedPredicate().getLeftHandExpression(), lastUpperBound.getParsedPredicate().getLeftHandColumn());
								pp.setOperator(Predicate.Operator.EQUAL);
								pp.setRightHandSide(lastUpperBound.getParsedPredicate().getRightHandLiteral());
								atom = new LocalPredicateAtom(pp, col, lastUpperBound.getLiteral());
								preds.set(i, atom);
								lastEquality = atom;
								continue;
							}
							else {
								// bound not included, contradiction
								return PredicateTruth.ALWAYS_FALSE;
							}
						}
					}
					
					// compare against other ranges
					if (lastLowerBoundIndex != -1) {
						LocalPredicateAtom lastLowerBound = 
							(LocalPredicateAtom) preds.get(lastLowerBoundIndex);
						// either we are the same, or the lower one is redundant
						int cmp = atom.getLiteral().compareTo(lastLowerBound.getLiteral());
						if (cmp > 0) {
							// we are greater, replace the last lower bound
							preds.set(lastLowerBoundIndex, null);
							lastLowerBoundIndex = i;
						}
						else {
							// we are lower and hence redundant
							preds.set(i, null);
						}
					}
					else {
						lastLowerBoundIndex = i;
					}
				}
			}	
		}
		
		// check for the latest column if we have an upper and lower bound on
		// the previous column and create a between predicate
		if (lastLowerBoundIndex != -1 && lastUpperBoundIndex != -1) {
			LocalPredicateAtom lb = 
				(LocalPredicateAtom) preds.get(lastLowerBoundIndex);
			LocalPredicateAtom ub = 
				(LocalPredicateAtom) preds.get(lastUpperBoundIndex);
			LocalPredicateBetween between = new LocalPredicateBetween(currentColumn,
					lb.getParsedPredicate(), ub.getParsedPredicate(),
					lb.getLiteral(), ub.getLiteral());
			
			preds.set(lastLowerBoundIndex, between);
			preds.set(lastUpperBoundIndex, null);
		}
		
		// remove the null elements from the preds list
		int target = 0;
		for (int source = 0; source < preds.size(); source++) {
			if (preds.get(source) != null) {
				preds.set(target++, preds.get(source));
			}
		}
		
		if (target == 0) {
			// this happens, if all elements where skipped because they were
			// always true. Then the whole conjunction is always true
			return PredicateTruth.ALWAYS_TRUE;
		}
		else {
			// now, set the predicates back for the conjunction
			preds = preds.subList(0, target);
			conjunct.setPredicates(preds);
			return PredicateTruth.UNKNOWN;
		}
	}

	/**
	 * Checks if the given graph is contiguous.
	 * 
	 * @param nodes The graph's nodes.
	 * @param edges The graph's edges.
	 * @return True, if the graph is contiguous, false if not.
	 */
	private final boolean isGraphContiguous(Relation[] nodes,
			List<JoinGraphEdge> edges)
	{
		// check for the trivial case where no join happens
		if (edges.isEmpty()) {
			return nodes.length == 1;
		}
		
		// prepare a set with all nodes
		Set<Relation> set = new HashSet<Relation>();
		for (int i = 0; i < nodes.length; i++) {
			set.add(nodes[i]);
		}
		
		Relation p = set.iterator().next();
		set.remove(p);
		recursiveVisitDepthFirst(p, set, edges);

		// graph is contiguous, if the traversal removed every node from the set
		return set.isEmpty();
	}
	
	/**
	 * Utility function that traverses a graph depth first and removes every visited node
	 * from a set.
	 * 
	 * @param current The current node in the traversal.
	 * @param nodes The set of nodes to remove traversed nodes from.
	 * @param edges The set of edges.
	 */
	private void recursiveVisitDepthFirst(Relation current,
			Set<Relation> nodes, List<JoinGraphEdge> edges)
	{
		for (int i = 0; i < edges.size(); i++) {
			JoinGraphEdge e = edges.get(i);
			if (e.getLeftNode() == current) {
				Relation next = e.getRightNode();
				if (nodes.remove(next)) {
					// operator was still contained, use it for the next traversal
					recursiveVisitDepthFirst(next, nodes, edges);
				}
			}
			else if (e.getRightNode() == current) {
				Relation next = e.getLeftNode();
				if (nodes.remove(next)) {
					// operator was still contained, use it for the next traversal
					recursiveVisitDepthFirst(next, nodes, edges);
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  Utility Methods
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a copy of the given predicate conjunction where the sides are switched for all atoms.
	 * For all contained atoms, the left hand side becomes the right hand side and vice versa.
	 * 
	 * @param conj The conjunct to create the side switched copy of.
	 * @return The side switched copy.
	 */
	public static JoinPredicateConjunct createSideSwitchedCopy(JoinPredicateConjunct conj)
	{
		JoinPredicateConjunct newConjunct = new JoinPredicateConjunct();
		for (JoinPredicateAtom p : conj.getConjunctiveFactors()) {
			newConjunct.addJoinPredicate(createSideSwitchedCopy(p));
		}
		return newConjunct;
	}
	
	
	/**
	 * Creates a copy of the given predicate atom where the sides are switched, i.e. the left
	 * hand side becomes the right hand side and vice versa.
	 * 
	 * @param atom The atom to create the side switched copy of.
	 * @return The side switched copy.
	 */
	public static JoinPredicateAtom createSideSwitchedCopy(JoinPredicateAtom atom)
	{
		Predicate oldPred = atom.getParsedPredicate();
		Predicate pp = new Predicate();
		pp.setLeftHandSide(oldPred.getRightHandExpression(), oldPred.getRightHandColumn());
		pp.setRightHandSide(oldPred.getLeftHandExpression(), oldPred.getLeftHandColumn());
		pp.setOperator(oldPred.getOp().getSideSwitched());
		
		JoinPredicateAtom na = new JoinPredicateAtom(pp,
				atom.getRightHandOriginatingTable(), 
				atom.getLeftHandOriginatingTable(),
				atom.getRightHandColumn(),
				atom.getLeftHandColumn());
		
		return na;
	}

	
	// --------------------------------------------------------------------------------------------
	//                                    Helper Classes
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * Utility class describing a pair of a <tt>JoinGraphEdge</tt> and a 
	 * <tt>JoinPredicateAtom</tt>.
	 */
	private static class TableOpAtomPair
	{
		/**
		 * The table scan operator.
		 */
		private Relation src;
		
		/**
		 * The predicate.
		 */
		private JoinPredicateAtom predicate;
		
		
		/**
		 * Creates a new plan operator/predicate pair.
		 * 
		 * @param pop The plan operator.
		 * @param predicate The predicate.
		 */
		public TableOpAtomPair(Relation src, JoinPredicateAtom predicate)
		{
			this.src = src;
			this.predicate = predicate;
		}

		/**
		 * Gets the plan operator from this TableOpAtomPair.
		 *
		 * @return The TableScanPlanOperator.
		 */
		public Relation getPop()
		{
			return this.src;
		}

		/**
		 * Gets the predicate from this JoinEdgePredAtomPair.
		 *
		 * @return The predicate.
		 */
		public JoinPredicateAtom getPredicate()
		{
			return this.predicate;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object o)
		{
			if (o == null || !(o instanceof TableOpAtomPair)) {
				return false;
			}
			
			TableOpAtomPair other = (TableOpAtomPair) o;
			return other.src.equals(this.src) && other.predicate.equals(this.predicate);
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode()
		{
			return this.src.hashCode() ^ this.predicate.hashCode();
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return "(" + this.src.toString() + ", " + this.predicate.toString() + ")";
		}
	}
	
	/**
	 * A comparator to sort local predicates after the following criteria in the
	 * given order of priority
	 * <ul>
	 * <li>Atoms first, then others.</li>
	 * <li>Within the atoms, by column index</li>
	 * <li>Within columns of the column index, by operator type as given
	 *     by the <i>ordinal</i> in the <tt>Predicate.Operator</tt> enumeration.</li>
	 * </ul>
	 */
	private static class PredicateSorter implements Comparator<LocalPredicate>
	{
		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(LocalPredicate o1, LocalPredicate o2)
		{
			if (o1 instanceof LocalPredicateAtom) {
				if (o2 instanceof LocalPredicateAtom) {
					int c1 = ((LocalPredicateAtom) o1).getColumn().getColumnIndex();
					int c2 = ((LocalPredicateAtom) o2).getColumn().getColumnIndex();
					 if (c1 < c2) {
						 return -1;
					 }
					 else if (c1 > c2) {
						 return 1;
					 }
					 else {
						int ord1 = ((LocalPredicateAtom) o1).getParsedPredicate().getOp().ordinal();
						int ord2 = ((LocalPredicateAtom) o2).getParsedPredicate().getOp().ordinal();
						return (ord1 < ord2) ? -1 : (ord1 > ord2) ? 1 : 0;
					 }
				}
				else {
					return -1;
				}
			}
			else if (o2 instanceof LocalPredicateAtom) {
				return 1;
			}
			else {
				return 0;
			}
		}
	}
	
	
	/**
	 * A utility class implementing a comparator that sorts edges after the index of the
	 * left hand side table scan operator. If the left table's index is the same, it
	 * sorts secondarily after the right hand table's index.
	 */
	private static class JoinEdgeSorter implements Comparator<JoinGraphEdge>
	{
		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(JoinGraphEdge e1, JoinGraphEdge e2)
		{
			int id1 = e1.getLeftNode().getID();
			int id2 = e2.getLeftNode().getID();
			
			if (id1 < id2) {
				return -1;
			}
			else if (id1 > id2) {
				return 1;
			}
			else {
				int rid1 = e1.getRightNode().getID();
				int rid2 = e2.getRightNode().getID();
				return rid1 < rid2 ? -1 : (rid1 > rid2 ? 1 : 0);
			}
		}
	}
}
