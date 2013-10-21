package de.tuberlin.dima.minidb.parser.solution;


import java.util.LinkedList;


import de.tuberlin.dima.minidb.parser.Column;
import de.tuberlin.dima.minidb.parser.DeleteQuery;
import de.tuberlin.dima.minidb.parser.Expression;
import de.tuberlin.dima.minidb.parser.FromClause;
import de.tuberlin.dima.minidb.parser.GroupByClause;
import de.tuberlin.dima.minidb.parser.HavingClause;
import de.tuberlin.dima.minidb.parser.InsertQuery;
import de.tuberlin.dima.minidb.parser.IntegerLiteral;
import de.tuberlin.dima.minidb.parser.Literal;
import de.tuberlin.dima.minidb.parser.OrderByClause;
import de.tuberlin.dima.minidb.parser.OrderColumn;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.ParseException;
import de.tuberlin.dima.minidb.parser.ParseTreeNode;
import de.tuberlin.dima.minidb.parser.ParsedQuery;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.parser.RealLiteral;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.parser.SQLTokenizer;
import de.tuberlin.dima.minidb.parser.SelectClause;
import de.tuberlin.dima.minidb.parser.SelectQuery;
import de.tuberlin.dima.minidb.parser.SetClause;
import de.tuberlin.dima.minidb.parser.StringLiteral;
import de.tuberlin.dima.minidb.parser.TableReference;
import de.tuberlin.dima.minidb.parser.Token;
import de.tuberlin.dima.minidb.parser.UpdateQuery;
import de.tuberlin.dima.minidb.parser.ValuesClause;
import de.tuberlin.dima.minidb.parser.WhereClause;
import de.tuberlin.dima.minidb.parser.Expression.NumericalOperator;
import de.tuberlin.dima.minidb.parser.OrderColumn.Order;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.parser.ParseException.ErrorCode;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.parser.Predicate.PredicateType;
import de.tuberlin.dima.minidb.parser.Token.TokenType;
import de.tuberlin.dima.minidb.parser.reduce.AdditionReduceExpression;
import de.tuberlin.dima.minidb.parser.reduce.DivisionReduceExpression;
import de.tuberlin.dima.minidb.parser.reduce.MultiplicationReduceExpression;
import de.tuberlin.dima.minidb.parser.reduce.ReduceExpression;
import de.tuberlin.dima.minidb.parser.reduce.SubtractionReduceExpression;
import de.tuberlin.dima.minidb.util.Pair;


/**
 * Implementation of the SQL parser as specified by the interface
 * <code>SQLParser</code>.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Michael Saecker (extended for expression support)
 */
public class SQLParserImpl implements SQLParser
{
	
	/**
	 * The statement for this parser.
	 */
	protected final String sqlStatement; 
	

	
	/**
	 * Creates a plain parser for the statement that starts at position 0.
	 * 
	 * @param statement The SQL statement to be parsed.
	 */
	public SQLParserImpl(String statement)
	{
		this.sqlStatement = statement;
	}


	/* (non-Javadoc) 
	 * @see de.tuberlin.dima.minidb.parser.SQLParser#parse()
	 */
	@Override
	public ParsedQuery parse() throws ParseException
	{
		// the tokenizer will cut the statement into tokens
		SQLTokenizer tokenizer = new SQLTokenizer(this.sqlStatement);
		Token currentToken = tokenizer.nextToken();
		
		// pure SELECT query 
		if (currentToken.getType() == TokenType.SELECT) {
			SelectQuery select = new SelectQuery();
			parseSelectQuery(tokenizer, select);
			expectCurrentToken(tokenizer, TokenType.END_OF_STATEMENT);
			return select;
		}
		// INSERT query
		else if (currentToken.getType() == TokenType.INSERT) {
			InsertQuery insert = new InsertQuery();
			parseInsertQuery(tokenizer, insert);
			expectCurrentToken(tokenizer, TokenType.END_OF_STATEMENT);
			return insert;
		}
		// UPDATE query
		else if (currentToken.getType() == TokenType.UPDATE) {
			UpdateQuery update = new UpdateQuery();
			parseUpdateQuery(tokenizer, update);			
			expectCurrentToken(tokenizer, TokenType.END_OF_STATEMENT);
			return update;
		}
		// DELETE query
		else if (currentToken.getType() == TokenType.DELETE) {
			DeleteQuery delete = new DeleteQuery();
			parseDeleteQuery(tokenizer, delete);
			expectCurrentToken(tokenizer, TokenType.END_OF_STATEMENT);
			return delete;
		}
		else {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
					tokenizer.getLastPosition(), currentToken.toString()); 
		}

		
	}
	
	
	/**
	 * Parses a SQL select query.
	 * 
	 * @param tokenizer The tokenizer from which to draw the tokens.
	 * @param query The query node into which to put the parsed contents.
	 * @throws ParseException If the string in the tokenizer does not represent a valid
	 *                        SELECT query.
	 */
	protected void parseSelectQuery(SQLTokenizer tokenizer, SelectQuery query)
	throws ParseException
	{
		Token t = null;
		
		// --------------------------------------------------
		//                   SELECT clause
		// --------------------------------------------------
		
		SelectClause sClause = new SelectClause();
		expectCurrentToken(tokenizer, TokenType.SELECT);
		
		do
		{
			t = tokenizer.nextToken();
			
			if (t.isAggregationType())
			{
				OutputColumn.AggregationType at = OutputColumn.AggregationType.getAggregationType(t);
				if (at == null) {
					throw new IllegalStateException("Parser is in illegal internal state.");
				}
				
				expectNextToken(tokenizer, TokenType.PARENTHESIS_OPEN);
				
				tokenizer.nextToken();
				Pair<ParseTreeNode, ParseTreeNode> tuple = parseExpression(tokenizer, true, false);
				if(!(tuple.getSecond() instanceof Column))
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT,
							tokenizer.getLastPosition(), tuple.getSecond().getNodeContents(),
							"<agg_function> ( <tab>.<col> ) AS <IDENTIFIER>");
				}
				
				expectCurrentToken(tokenizer, TokenType.PARENTHESIS_CLOSE);
				expectNextToken(tokenizer, TokenType.AS);
				
				String colAlias = getAndCheckToken(tokenizer,
										TokenType.IDENTIFIER, "<column alias>");
				sClause.addOutputColumn(new OutputColumn((Column) tuple.getSecond(), colAlias, at, tuple.getFirst()));
			}
			else if (t.getType() == TokenType.IDENTIFIER ||
					t.getType() == TokenType.PARENTHESIS_OPEN ||
					t.getType() == TokenType.LITERAL ||
					t.getType() == TokenType.INTEGER_NUMBER ||
					t.getType() == TokenType.REAL_NUMBER) {
				// Column col = parseColumn(tokenizer);
				Pair<ParseTreeNode, ParseTreeNode> tuple = parseExpression(tokenizer, true, false);
				if(!(tuple.getSecond() instanceof Column))
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT,
							tokenizer.getLastPosition(), tuple.getSecond().getNodeContents(),
							"<tab>.<col> AS <IDENTIFIER>");
				}
				expectCurrentToken(tokenizer, TokenType.AS);
				
				String colAlias = getAndCheckToken(tokenizer,
										TokenType.IDENTIFIER, "<column alias>");
				
				sClause.addOutputColumn(new OutputColumn((Column) tuple.getSecond(), colAlias, AggregationType.NONE, tuple.getFirst()));
			}
			else {
				throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
						tokenizer.getLastPosition(), t.getTokenString(), "<output column>");
			}
			
		}
		while ( (t = tokenizer.nextToken()).getType() == TokenType.COMMA);
		
		query.setSelectClause(sClause);
		
		// --------------------------------------------------
		//                    FROM clause
		// --------------------------------------------------
		
		expectCurrentToken(tokenizer, TokenType.FROM);
		FromClause fClause = new FromClause();
		
		do {
			t = tokenizer.nextToken();
			if(t.getType() == TokenType.PARENTHESIS_OPEN)
			{
				// parse select query
				SelectQuery select = new SelectQuery();
				tokenizer.nextToken();
				parseSelectQuery(tokenizer, select);
				expectCurrentToken(tokenizer, TokenType.PARENTHESIS_CLOSE);
				String tabAlias = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table alias>");
				fClause.addTable(new TableReference(select, tabAlias));
				
			} else 
			{
				if(t.getType() == TokenType.IDENTIFIER)
				{
					String tabName = t.getTokenString();
					String tabAlias = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table alias>");
					fClause.addTable(new TableReference(tabName, tabAlias));
				} else 
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
							tokenizer.getLastPosition(), t.getTokenString(), "<table name>");
				}
			}
		}
		while ( (t = tokenizer.nextToken()).getType() == TokenType.COMMA);
		
		query.setFromClause(fClause);
		
		// --------------------------------------------------
		//                    WHERE clause
		// --------------------------------------------------
		
		// the where clause is optional, we may skip it
		if (t.getType() == TokenType.WHERE)
		{
			WhereClause wClause = new WhereClause();
			do {
				int tempPos = tokenizer.getLastPosition();
				
				tokenizer.nextToken();
				Predicate pred = parsePredicate(tokenizer, false);
				if ( pred.getType() != Predicate.PredicateType.COLUMN_COLUMN && 
				     pred.getType() != Predicate.PredicateType.COLUMN_LITERAL)
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
						tempPos, pred.getNodeContents(),
						"[<tab>.<col> <op> <literal>] || [<tab>.<col> <op> <tab>.<col>]");
				}
				
				wClause.addPredicate(pred);
			}
			while ( (t = tokenizer.getLastToken()).getType() == TokenType.AND);
			
			query.setWhereClause(wClause);
		}
		
		// --------------------------------------------------
		//                    GROUP BY clause
		// --------------------------------------------------
		
		// the group by clause is optional, we may skip it
		if (t.getType() == TokenType.GROUP) {
			expectNextToken(tokenizer, TokenType.BY);
			GroupByClause gbClause = new GroupByClause();
			do {
				t = tokenizer.nextToken();
				Column col = parseColumn(tokenizer);
				gbClause.addColumn(col);
			}
			while ( (t = tokenizer.nextToken()).getType() == TokenType.COMMA);
			
			query.setGroupByClause(gbClause);
		}
		
		// --------------------------------------------------
		//                     HAVING clause
		// --------------------------------------------------
		
		// the having clause is optional, we may skip it
		if (t.getType() == TokenType.HAVING)
		{
			HavingClause hClause = new HavingClause();
			do {
				int tempPos = tokenizer.getLastPosition();
				
				tokenizer.nextToken();
				Predicate pred = parsePredicate(tokenizer, true);
				if ( pred.getType() != Predicate.PredicateType.ALIASCOLUMN_LITERAL) {
						throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
							tempPos, pred.getNodeContents(), "[<col alias> <op> <literal>]");
					}
				hClause.addPredicate(pred);
			}
			while ( (t = tokenizer.getLastToken()).getType() == TokenType.AND);
			
			query.setHavingClause(hClause);
		}
		
		// --------------------------------------------------
		//                    ORDER BY clause
		// --------------------------------------------------
		
		// the group by clause is optional, we may skip it
		if (t.getType() == TokenType.ORDER) {
		    
			expectNextToken(tokenizer, TokenType.BY);
			OrderByClause obClause = new OrderByClause();
			do {
				String colAlias = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, 
																		"<column alias>");
				OutputColumn outCol = new OutputColumn(null, colAlias);
				
				t = tokenizer.nextToken();
				if (t.getType() == TokenType.ASCENDING) {
					obClause.addOrderColumn(new OrderColumn
							(outCol, OrderColumn.Order.ASCENDING));
					t = tokenizer.nextToken();
				}
				else if (t.getType() == TokenType.DESCENDING) {
					obClause.addOrderColumn(new OrderColumn
							(outCol, OrderColumn.Order.DESCENDING));
					t = tokenizer.nextToken();
				}
				else {
					obClause.addOrderColumn(new OrderColumn(outCol));
				}
			}
			while (t.getType() == TokenType.COMMA);
			
			query.setOrderByClause(obClause);
		}
		
		// --------------------------------------------------
		//                    END OF QUERY
		// --------------------------------------------------		
	}
	

	/**
	 * Parses a SQL insert query.
	 * 
	 * @param tokenizer The tokenizer from which to draw the tokens.
	 * @param query The query node into which to put the parsed contents.
	 * @throws ParseException If the string in the tokenizer does not represent a valid
	 *                        INSERT query.
	 */
	protected void parseInsertQuery(SQLTokenizer tokenizer, InsertQuery query)
	throws ParseException
	{
		// --------------------------------------------------
		//                   INSERT query
		// --------------------------------------------------
		
		expectCurrentToken(tokenizer, TokenType.INSERT);
		expectNextToken(tokenizer, TokenType.INTO);
		
		// get the table name
		query.setTableName(getAndCheckToken(
				tokenizer, TokenType.IDENTIFIER, "<table name>"));
		
		// make a distinction between cases where a SELECT query follows and where
		// a set of VALUES clauses follows.
		Token t = tokenizer.nextToken();
		if (t.getType() == TokenType.SELECT) {
			// select case. what follows is a complete query.
			SelectQuery select = new SelectQuery();
			parseSelectQuery(tokenizer, select);
			query.setSelectQuery(select);			
		}
		else if (t.getType() == TokenType.VALUES) {
			
			// parse list of values clauses
			do {
				// clause begins with opening parenthesis
				expectNextToken(tokenizer, TokenType.PARENTHESIS_OPEN);
				
				ValuesClause values = new ValuesClause();
				
				// go over the list of values in this clause here
				do {
					// inside are literals
					t = tokenizer.nextToken();
					Literal lit = getLiteralForToken(t);
					if (lit == null) {
						throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
								tokenizer.getLastPosition(), t.getTokenString(),
								"<string literal> | <integer number> | <real number>");
					}
					
					values.addValue(lit);
				}
				while ((t = tokenizer.nextToken()).getType() == TokenType.COMMA);
				
				// clause ends with closing parenthesis
				expectCurrentToken(tokenizer, TokenType.PARENTHESIS_CLOSE);
				query.addValuesClause(values);
			}
			while ((t = tokenizer.nextToken()).getType() == TokenType.COMMA);
			
		}
		else {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
					tokenizer.getLastPosition(), t.getTokenString(), "VALUES || SELECT");
		}
	}
	
	/**
	 * Parses a SQL update query.
	 * 
	 * @param tokenizer The tokenizer from which to draw the tokens.
	 * @param query The query node into which to put the parsed contents.
	 * @throws ParseException If the string in the tokenizer does not represent a valid
	 *                        UPDATE query.
	 */
	protected void parseUpdateQuery(SQLTokenizer tokenizer, UpdateQuery query)
	throws ParseException
	{

		// --------------------------------------------------
		//                   UPDATE clause
		// --------------------------------------------------
		expectCurrentToken(tokenizer, TokenType.UPDATE);

		// get the table name
		String tabName = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table name>");
		String tabAlias = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table alias>");
		
		query.setTable(new TableReference(tabName, tabAlias));
		
		// --------------------------------------------------
		//                   SET clause
		// --------------------------------------------------
		expectNextToken(tokenizer, TokenType.SET);
				
		// go over the list of equality predicates in this list
		Token t;
		SetClause set = new SetClause();
		do {
			t = tokenizer.nextToken();
			// <column> = <expr> | <column> = <value>
			Predicate pred = parsePredicate(tokenizer, false);
			if (pred == null || pred.getOp() != Predicate.Operator.EQUAL || 
				(pred.getType() != Predicate.PredicateType.COLUMN_LITERAL &&
				 pred.getType() != Predicate.PredicateType.COLUMN_COLUMN)
				) {
				throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
						tokenizer.getLastPosition(), t.getTokenString(),
						"{ <column> = { <string literal> | <integer number> | <real number> } } | " +
						"{ <column> = <column_expr> } | { <column> = <expr> }"
						);
			}
			set.addEqualityPredicate(pred);
		}
		while ((t = tokenizer.getLastToken()).getType() == TokenType.COMMA);
		query.setSetClause(set);
		
		// --------------------------------------------------
		//                   WHERE clause
		// --------------------------------------------------
		if ((tokenizer.getLastToken()).getType() == TokenType.WHERE)
		{
			WhereClause wClause = new WhereClause();
			do {
				int tempPos = tokenizer.getCurrentPosition();
				
				tokenizer.nextToken();
				Predicate pred = parsePredicate(tokenizer, false);
				if (pred.getType() != Predicate.PredicateType.COLUMN_LITERAL)
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
						tempPos, pred.getNodeContents(), "[<tab>.<col> <op> <literal>]");
				}
				
				wClause.addPredicate(pred);
			}
			while ( (tokenizer.getLastToken()).getType() == TokenType.AND);
			
			query.setWhereClause(wClause);
		}
	}
	
	/**
	 * Parses a SQL delete query.
	 * 
	 * @param tokenizer The tokenizer from which to draw the tokens.
	 * @param query The query node into which to put the parsed contents.
	 * @throws ParseException If the string in the tokenizer does not represent a valid
	 *                        DELETE query.
	 */
	protected void parseDeleteQuery(SQLTokenizer tokenizer, DeleteQuery query)
	throws ParseException
	{
		// --------------------------------------------------
		//                   DELETE clause
		// --------------------------------------------------
		
		
		expectCurrentToken(tokenizer, TokenType.DELETE);
		expectNextToken(tokenizer, TokenType.FROM);
		
		// get the table name
		String tabName = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table name>");
		String tabAlias = getAndCheckToken(tokenizer, TokenType.IDENTIFIER, "<table alias>");
		
		query.setTable(new TableReference(tabName, tabAlias));
		
		// --------------------------------------------------
		//                    WHERE clause
		// --------------------------------------------------
		
		// where clause is optional
		if ((tokenizer.nextToken()).getType() == TokenType.WHERE)
		{
			WhereClause wClause = new WhereClause();
			do {
				int tempPos = tokenizer.getCurrentPosition();
				
				tokenizer.nextToken();
				Predicate pred = parsePredicate(tokenizer, false);
				if (pred.getType() != Predicate.PredicateType.COLUMN_LITERAL)
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
						tempPos, pred.getNodeContents(), "[<tab>.<col> <op> <literal>]");
				}
				
				wClause.addPredicate(pred);
			}
			while ( (tokenizer.getLastToken()).getType() == TokenType.AND);
			
			query.setWhere(wClause);
		}

	}
	
	/**
	 * Parses a column from the tokenizer: <i>tab.col</i>.
	 * The tokenizer is expected to have started the parsing of the column such
	 * that the first token is retrieved through accessing the last token rather
	 * than getting a new token.
	 * 
	 * @param tokenizer The tokenizer that gives the column tokens.
	 * @return The parsed column.
	 * @throws ParseException If the next three tokens of the tokenizer do not describe a
	 *                        valid column.
	 */
	private Column parseColumn(SQLTokenizer tokenizer)
	throws ParseException
	{
		Token t = tokenizer.getLastToken();
		
		if (t.getType() != TokenType.IDENTIFIER) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT, 
					tokenizer.getLastPosition(), t.toString(), "<table name>");
		}
		
		String tabName = t.getTokenString();
		
		if ((t = tokenizer.nextToken()).getType() != TokenType.PERIOD) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT,
					tokenizer.getLastPosition(), t.toString(), ".");
		}
		
		if ((t = tokenizer.nextToken()).getType() != TokenType.IDENTIFIER)
		{
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT, 
					tokenizer.getLastPosition(), t.toString(), "<column name>");
		}
		
		return new Column(t.getTokenString(), tabName);
	}
	
	/**
	 * Parses a predicate from the tokenizer and evaluates constant expressions. Predicates are of the following type:
	 * 
	 * <ul>
	 *   <li><i>tab.col op literal<i></li>
	 *   <li><i>tabA.colC op tabB.ColD</i></li>
	 *   <li><i>col op literal</i></li>
	 * </ul>
	 * 
	 * The tokenizer is expected to have started the parsing of the predicate
	 * such that the first token is retrieved through accessing the last token
	 * rather than getting a new token.
	 * 
	 * @param tokenizer The tokenizer that gives the predicate tokens.
	 * @param inHaving Sets whether the predicate is in the HAVING clause or not
	 * @return The parsed predicate.
	 * @throws ParseException If the next three tokens of the tokenizer do not describe a
	 *                        valid predicate.
	 */
	public Predicate parsePredicate(SQLTokenizer tokenizer, boolean inHaving)
	throws ParseException
	{		
		Predicate pred = new Predicate();
		
		// parse left expression of predicate
		Pair<ParseTreeNode, ParseTreeNode> leftTuple = parseExpression(tokenizer, true, inHaving);
		pred.setLeftHandSide(leftTuple.getFirst(), leftTuple.getSecond());
		
		// set operator
		Token t = tokenizer.getLastToken();
		if (t.isOperatorType())
		{
			pred.setOperator(getOperatorForToken(t));
		} else {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_COLUMN_FORMAT, 
					tokenizer.getLastPosition(), t.toString(), "<column name> | <operator>");			
		}
		
		// advance token because parseExpression expects the token to be already consumed (lastToken)
		tokenizer.nextToken();
		
		// parse right expression of predicate
		Pair<ParseTreeNode, ParseTreeNode> rightTuple = parseExpression(tokenizer, false, inHaving);
		pred.setRightHandSide(rightTuple.getFirst(), rightTuple.getSecond());
		
		return pred;
	}


	/**
	 * Parses an expression and reduces constant parts of it by evaluating them.
	 * 
	 * @param tokenizer The tokenizer that gives the predicate tokens.
	 * @param mustContainColumn Set whether the expression has to contain at least one column
	 * @param inHavingClause Set whether the expression should be parsed as where or having expression
	 * @return Tuple of the parsed expression and the contained column(where/select) or output column(having)
	 * @throws ParseException
	 */
	private Pair<ParseTreeNode, ParseTreeNode> parseExpression(SQLTokenizer tokenizer, boolean mustContainColumn, boolean inHavingClause)
	throws ParseException 
	{	
		Token t = tokenizer.getLastToken();
		ParseTreeNode column = null;
		
		// sanity check
		if (t.getType() != TokenType.IDENTIFIER &&
				t.getType() != TokenType.PARENTHESIS_OPEN &&
				t.getType() != TokenType.LITERAL &&
				t.getType() != TokenType.INTEGER_NUMBER &&
				t.getType() != TokenType.REAL_NUMBER
				) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
					tokenizer.getLastPosition(), t.toString(), "<table alias> | <column alias> | <agg_function> | ( | <literal> ");
		}

		// list to handle terms in parentheses / low priority terms
		// combining the trees from right to left will create overall expression tree
		LinkedList<Expression> exprList = new LinkedList<Expression>();
		Token lastToken = null;
		Expression current = new Expression();

		do 
		{
			// literal or column/alias
			if (lastToken != null && lastToken.getType() == TokenType.LITERAL)
			{
				// literal cannot be combined with anything else yet
				throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
						tokenizer.getLastPosition(), t.toString(), "<literal> must stand alone");
			} else if (t.getType() == TokenType.LITERAL && lastToken == null)
			{
				// string literal
				current.setLeftBranch(getLiteralForToken(t));
			} else if (t.getType() == TokenType.INTEGER_NUMBER || t.getType() == TokenType.REAL_NUMBER || t.getType() == TokenType.IDENTIFIER) 
			{
				ParseTreeNode expr = null;
				if(t.getType() == TokenType.INTEGER_NUMBER || t.getType() == TokenType.REAL_NUMBER)
				{
					expr = getLiteralForToken(t);
				} else if (t.getType() == TokenType.IDENTIFIER)
				{
					if(column != null)
					{
						throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
								tokenizer.getLastPosition(), t.toString(), "multiple columns in expression");
					} else if (inHavingClause)
					{
						// only alias allowed, no table name
						if (lastToken != null && (lastToken.getType() == TokenType.IDENTIFIER || lastToken.getType() == TokenType.PERIOD))
						{
							throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
									tokenizer.getLastPosition(), t.toString(), "<IDENTIFIER> <IDENTIFIER> in HAVING clause");
						} else 
						{
							expr = new OutputColumn(null, t.getTokenString());
							column = expr;
						}
					} else 
					{
						expr = parseColumn(tokenizer);
						t = tokenizer.getLastToken();
						column = expr;
					}
				}
					
				// start of column expression 
				if (current.getOperator() == null && (lastToken == null || lastToken.isNumericalOperatorType() || lastToken.getType() == TokenType.PARENTHESIS_OPEN)) 
				{
					current.setLeftBranch(expr);
				} else if (lastToken.isNumericalOperatorType()) // check that two literals are not succeeding each other 
				{
					if(current.getOperator() != NumericalOperator.PLUS) 
					{
						// multiplication/division/subtraction can be added to right branch (priority over addition/subtraction)
						current.setRightBranch(expr);
						Expression col = new Expression();
						col.setLeftBranch(current);
						current = col;
					} else 
					{
						// addition splits the tree
						current.setLeftBranch(expr);
					}
				} else 
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
							tokenizer.getLastPosition(), t.toString(), "<literal> <literal>");
				}
			} else if (t.isNumericalOperatorType()) 
			{
				// two numerical operators following each other
				if (lastToken == null || lastToken.isNumericalOperatorType()) 
				{
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
							tokenizer.getLastPosition(), t.toString(), "<numerical_operator> <numerical_operator>");
				} else // lastToken was literal or column 
				{
					current.setOperator(getNumericalOperatorForToken(t));
					if(current.getOperator() == NumericalOperator.PLUS)
					{
						exprList.push(current);
						current = new Expression();
					}
				}				
			} else if (t.getType() == TokenType.PARENTHESIS_OPEN) 
			{
				// branch of a new subtree for the expression in parentheses
				tokenizer.nextToken();
				Pair<ParseTreeNode, ParseTreeNode> par = parseExpression(tokenizer, false, inHavingClause);
				expectCurrentToken(tokenizer, TokenType.PARENTHESIS_CLOSE);
				if(par.getSecond() != null){
					column = par.getSecond();
				}
				if(current.getOperator() == null)
				{
					current.setLeftBranch(par.getFirst());
				} else {
					current.setRightBranch(par.getFirst());
					Expression col = new Expression();
					col.setLeftBranch(current);
					current = col;
				}
			} else {
				throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
						tokenizer.getLastPosition(), t.toString(), "<expr> | <column_expr> expected.");
			}
			
			lastToken = t;
			// include WHERE and COMMA in list cause of UPDATE statements, AS because of SELECT clause expressions
		} while(!(t = tokenizer.nextToken()).isOperatorType() && t.getType() != TokenType.AND && 
				t.getType() != TokenType.WHERE && t.getType() != TokenType.AS && 
				t.getType() != TokenType.GROUP && t.getType() != TokenType.ORDER && 
				t.getType() != TokenType.END_OF_STATEMENT && t.getType() != TokenType.COMMA && 
				t.getType() != TokenType.PARENTHESIS_CLOSE // special case to parse expressions encapsulated by parentheses
				);
		
		// check expression did not end with a numerical operator
		if(current.getOperator() != null && (current.getLeftBranch() == null || current.getRightBranch() == null))
		{
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
					tokenizer.getLastPosition(), t.toString(), "<numerical_operator> <op>");
		}
		
		// check whether the column condition was fulfilled
		if(mustContainColumn && column == null)
		{
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
					tokenizer.getLastPosition(), t.toString(), "<column_expr> contains no column");			
		}
		
		// complete column expression by concatenating the elements in reverse
		while(exprList.size() > 0)
		{
			Expression col = exprList.pop();
			if(current.getOperator() == null)
			{
				ParseTreeNode branch = current.getLeftBranch();
				col.setRightBranch(branch);
				current = col;
			} else 
			{
				col.setRightBranch(current);
				current = col;
			}
		}
		
		// check if expression is simple (no right branch or operator for all branches)
		ParseTreeNode res;
		if (current.getRightBranch() == null && current.getOperator() != null)
		{
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT,
					tokenizer.getLastPosition(), t.toString(), "<expr> <numop> <expr>: second expression missing");			
		} else if (current.getRightBranch() == null)
		{
			if(current.getLeftBranch() instanceof Expression)
			{
				res = reduceExpression((Expression) current.getLeftBranch());
			} else 
			{
				res = current.getLeftBranch(); 				
			}
		} else {
			// reduce expression by evaluating constant parts of it
			res = reduceExpression(current);
		}
		
		return new Pair<ParseTreeNode, ParseTreeNode>(res, column);
	}
	
	/**
	 * Reduces the given expression by evaluating constant terms as far as possible in a recursive way.
	 * 
	 * @param input The expression to reduce.
	 * @return The reduced expression.
	 */
	private ParseTreeNode reduceExpression(Expression input)
	throws ParseException
	{
		if(input instanceof Expression)
		{
			// get operator of column expression
			ReduceExpression op = null;
			switch(input.getOperator())
			{
				case PLUS:
					op = new AdditionReduceExpression();
				break;
				case MINUS:
					op = new SubtractionReduceExpression();
				break;
				case MUL:
					op = new MultiplicationReduceExpression();
				break;
				case DIV:
					op = new DivisionReduceExpression();
				break;
				default:
					throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT);
			}
			
			// evaluate left branch of tree
			ParseTreeNode leftBranch = input.getLeftBranch();
			if(leftBranch instanceof Expression)
			{
				leftBranch = reduceExpression((Expression) leftBranch);
			}
	
			// evaluate right branch of tree
			ParseTreeNode rightBranch = input.getRightBranch();
			if(rightBranch instanceof Expression)
			{
				rightBranch = reduceExpression((Expression) rightBranch);
			}
			
			// reduce the expression if both branches are constants
			if(leftBranch instanceof IntegerLiteral && rightBranch instanceof IntegerLiteral)
			{
//				// treat integer division as real to avoid rounding (not wanted)
//				if(input.getOperator() == NumericalOperator.DIV)
//				{
//					// use double for division (otherwise result would be rounded)
//					return new RealLiteral(op.reduce((double)((IntegerLiteral)leftBranch).getNumber(), (double)((IntegerLiteral) rightBranch).getNumber()));
//				} 
//				else
//				{
					return new IntegerLiteral(op.reduce(((IntegerLiteral)leftBranch).getNumber(), ((IntegerLiteral) rightBranch).getNumber()));					
//				}
			} 
			else if (leftBranch instanceof RealLiteral && rightBranch instanceof IntegerLiteral)
			{
				return new RealLiteral(op.reduce(((RealLiteral)leftBranch).getNumber(), ((IntegerLiteral) rightBranch).getNumber()));
			} 
			else if (leftBranch instanceof IntegerLiteral && rightBranch instanceof RealLiteral)
			{
				return new RealLiteral(op.reduce(((IntegerLiteral)leftBranch).getNumber(), ((RealLiteral) rightBranch).getNumber()));				
			} 
			else if (leftBranch instanceof RealLiteral && rightBranch instanceof RealLiteral)
			{
				return new RealLiteral(op.reduce(((RealLiteral)leftBranch).getNumber(), ((RealLiteral) rightBranch).getNumber()));
			} 
			else if ((leftBranch instanceof RealLiteral || leftBranch instanceof IntegerLiteral) && 
					rightBranch instanceof Expression && 
					input.getOperator().isCommutative() && 
					input.getOperator() == ((Expression)rightBranch).getOperator())
			{
				// right branch is an expression "num_literal <num_op> col" or "col <num_op> num_literal" because
				// an expression can contain only one column
				// if the <num_ops> are identical and commutative the expression can be further reduced
				Expression right = (Expression) rightBranch;
				// reduce expression by rewiring the expressions
				if(right.getRightBranch() instanceof Literal)
				{
					right.swap();					
				}
				// get column of expression
				ParseTreeNode rightColChild = right.getRightBranch();
				// replace column with literal of left branch
				right.setRightBranch(leftBranch);
				// reduce num_lit <num_op> num_lit expression and create new expression with column and reduced value
				return new Expression(reduceExpression(right), rightColChild, input.getOperator());
			} 
			else if ((rightBranch instanceof RealLiteral || rightBranch instanceof IntegerLiteral) && 
						leftBranch instanceof Expression &&
						input.getOperator().isCommutative() &&
						input.getOperator() == ((Expression) leftBranch).getOperator())
			{
				// left branch is an expression: "num_literal <num_op> col" or "col <num_op> num_literal" because 
				// an expression can contain only one column
				// if the <num_ops> are identical and commutative the expression can be further reduced
				Expression left = (Expression) leftBranch;
				// reduce expression by rewiring the expressions
				if(left.getRightBranch() instanceof Literal)
				{
					left.swap();					
				}
				// get column of expression
				ParseTreeNode rightColChild = left.getRightBranch();
				// replace column with literal of right branch
				left.setRightBranch(rightBranch);
				// reduce num_lit <num_op> num_lit expression and create new expression with column and reduced value
				return new Expression(reduceExpression(left), rightColChild, input.getOperator());
			} 
			else 
			{
				return new Expression(leftBranch, rightBranch, input.getOperator());
			}
		} else // only ColumnExpressions should be called by reduce Expression
		{
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_PREDICATE_FORMAT);
		}
	}
	
	/**
	 * Gets the predicate operator corresponding to the given operator token.
	 *  
	 * @param t The token to evaluate.
	 * @return The operator type, or null, if the token does not describe an operator.
	 */
	private Predicate.Operator getOperatorForToken(Token t)
	{
		if (t.getType() == TokenType.OPERAND_EQUAL) {
			return Predicate.Operator.EQUAL;
		}
		else if (t.getType() == TokenType.OPERAND_UNEQUAL) {
			return Predicate.Operator.NOT_EQUAL;
		}
		else if (t.getType() == TokenType.OPERAND_GREATER_EQUAL) {
			return Predicate.Operator.GREATER_OR_EQUAL;
		}
		else if (t.getType() == TokenType.OPERAND_GREATER_THAN) {
			return Predicate.Operator.GREATER;
		}
		else if (t.getType() == TokenType.OPERAND_SMALLER_EQUAL) {
			return Predicate.Operator.SMALLER_OR_EQUAL;
		}
		else if (t.getType() == TokenType.OPERAND_SMALLER_THAN) {
			return Predicate.Operator.SMALLER;
		}
		else {
			return null; 
		}
	}

	/**
	 * Gets the numerical operator corresponding to the given operator token.
	 *  
	 * @param t The token to evaluate.
	 * @return The operator type, or null, if the token does not describe an operator.
	 */
	private Expression.NumericalOperator getNumericalOperatorForToken(Token t)
	{
		if (t.getType() == TokenType.MINUS) {
			return Expression.NumericalOperator.MINUS;
		}
		else if (t.getType() == TokenType.PLUS) {
			return Expression.NumericalOperator.PLUS;
		}
		else if (t.getType() == TokenType.MUL) {
			return Expression.NumericalOperator.MUL;
		}
		else if (t.getType() == TokenType.DIV) {
			return Expression.NumericalOperator.DIV;
		}
		else {
			return null; 
		}
	}	
	
	/**
	 * Builds a literal of the type of the token containing the token's data.
	 * 
	 * @param t The token containing the literal.
	 * 
	 * @return The literal for the type of the token.
	 */
	private Literal getLiteralForToken(Token t)
	{
		if (t.getType() == TokenType.LITERAL) {
			return new StringLiteral(t.getTokenString());
		}
		else if (t.getType() == TokenType.INTEGER_NUMBER) {
			try {
				long num = Long.parseLong(t.getTokenString());
				return new IntegerLiteral(num);
			}
			catch (NumberFormatException nfex) {
				throw new IllegalStateException
						("Integer literal contains no integer number");
			}
		}
		else if (t.getType() == TokenType.REAL_NUMBER) {
			try {
				double num = Double.parseDouble(t.getTokenString());
				return new RealLiteral(num);
			}
			catch (NumberFormatException nfex) {
				throw new IllegalStateException
						("Real literal contains no real number");
			}
		}
		else {
			return null;
		}
	}
	
	
	/**
	 * Checks the type of the tokenizer's last token. If the token is not of the
	 * given type, an exception is thrown with the expected type name in its message.
	 * 
	 * @param tokenizer The tokenizer to draw the token from.
	 * @param type The type that the token should have.
	 * 
	 * @throws ParseException Thrown, if the token is not of the requested type.
	 */
	private void expectCurrentToken(SQLTokenizer tokenizer, TokenType type)
	throws ParseException
	{
		expectCurrentToken(tokenizer, type, type.getStringRepresentation());
	}
	
	/**
	 * Checks the type of the tokenizer's last token. If the token is not of the
	 * given type, an exception is thrown with the expected type in its message.
	 * 
	 * @param tokenizer The tokenizer to draw the token from.
	 * @param type The type that the token should have.
	 * @param expected The string for the exception message, describing the expected type.
	 * 
	 * @throws ParseException Thrown, if the token is not of the requested type.
	 */
	private void expectCurrentToken(SQLTokenizer tokenizer, TokenType type, String expected)
	throws ParseException
	{
		Token t = tokenizer.getLastToken();
		if (t.getType() != type) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
					tokenizer.getLastPosition(), t.getTokenString(), expected);
		}
	}
	
	/**
	 * Draws a token from the optimizer and verifies its type. If the token is not of the
	 * given type, an exception is thrown with the expected type name in its message.
	 * 
	 * @param tokenizer The tokenizer to draw the token from.
	 * @param type The type that the token should have.
	 * 
	 * @throws ParseException Thrown, if the token is not of the requested type.
	 */
	private void expectNextToken(SQLTokenizer tokenizer, TokenType type)
	throws ParseException
	{
		expectNextToken(tokenizer, type, type.getStringRepresentation());
	}
	
	/**
	 * Draws a token from the optimizer and verifies its type. If the token is not of the
	 * given type, an exception is thrown with the expected type in its message.
	 * 
	 * @param tokenizer The tokenizer to draw the token from.
	 * @param type The type that the token should have.
	 * @param expected The string for the exception message, describing the expected type.
	 * 
	 * @throws ParseException Thrown, if the token is not of the requested type.
	 */
	private void expectNextToken(SQLTokenizer tokenizer, TokenType type, String expected)
	throws ParseException
	{
		Token t = tokenizer.nextToken();
		if (t.getType() != type) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
					tokenizer.getLastPosition(), t.getTokenString(), expected);
		}
	}
	
	/**
	 * Gets a token from the optimizer, verifies its type and returns its contents.
	 * 
	 * @param tokenizer The tokenizer to draw the token from.
	 * @param type The type that the token should have.
	 * @param expected The string for the exception message, describing the expected type.
	 * @return The contents of the token.
	 * 
	 * @throws ParseException Thrown, if the token is not of the requested type.
	 */
	private String getAndCheckToken(SQLTokenizer tokenizer, TokenType type, String expected)
	throws ParseException
	{
		Token t = tokenizer.nextToken();
		if (t.getType() != type) {
			throw new ParseException(this.sqlStatement, ErrorCode.INVALID_TOKEN,
					tokenizer.getLastPosition(), t.getTokenString(), expected);
		}
		
		return t.getTokenString();
	}
}
