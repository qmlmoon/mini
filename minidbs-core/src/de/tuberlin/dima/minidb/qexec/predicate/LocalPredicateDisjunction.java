package de.tuberlin.dima.minidb.qexec.predicate;


import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * A predicate representing a disjunction of other predicates.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class LocalPredicateDisjunction implements LocalPredicate
{
	/**
	 * The predicates that are part of the disjunction.
	 */
	private LocalPredicate[] lps;

	/**
	 * Creates a predicate representing a disjunction over the given predicates.
	 * 
	 * @param lps The predicates that are part of the disjunction.
	 */
	public LocalPredicateDisjunction(LocalPredicate[] lps)
	{
		this.lps = new LocalPredicate[lps.length];
		for (int i = 0; i < lps.length; i++) {
			if (lps[i] == null) {
				throw new NullPointerException("No null predicates allowed in disjunction.");
			}
			else {
				this.lps[i] = lps[i];
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple dataTuple) throws QueryExecutionException 
	{
		for (int i = 0; i < this.lps.length; i++) {
			if (this.lps[i].evaluate(dataTuple)) {
				return true;
			}
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.lps.length; i++) {
			bld.append(this.lps[i]);
			if (i != this.lps.length - 1) {
				bld.append(" OR ");
			}
		}
		bld.append(")");
		return bld.toString();
	}

}
