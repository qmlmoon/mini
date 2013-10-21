package de.tuberlin.dima.minidb.qexec.predicate;


import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * A predicate representing a conjunction of join predicates.
 * 
 * Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class JoinPredicateConjunction implements JoinPredicate
{
	/**
	 * The predicates that are part of the conjunction.
	 */
	private JoinPredicate[] predicates;

	/**
	 * Creates a predicate representing a conjunction over the given join predicates.
	 * 
	 * @param ps The predicates that are part of the conjunction.
	 */
	public JoinPredicateConjunction(JoinPredicate[] ps)
	{
		this.predicates = new JoinPredicate[ps.length];
		for (int i = 0; i < ps.length; i++) {
			if (ps[i] == null) {
				throw new NullPointerException("No null predicates allowed in conjunction.");
			}
			else {
				this.predicates[i] = ps[i];
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple, de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple leftHandSide, DataTuple rightHandSide) throws QueryExecutionException
	{
		for (int i = 0; i < this.predicates.length; i++) {
			if (!this.predicates[i].evaluate(leftHandSide, rightHandSide)) {
				return false;
			}
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.predicates.length; i++) {
			bld.append(this.predicates[i]);
			if (i != this.predicates.length - 1) {
				bld.append(" AND ");
			}
		}
		bld.append(")");
		return bld.toString();
	}
}
