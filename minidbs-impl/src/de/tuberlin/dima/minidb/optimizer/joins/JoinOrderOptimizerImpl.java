package de.tuberlin.dima.minidb.optimizer.joins;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;

public class JoinOrderOptimizerImpl implements JoinOrderOptimizer {

	private CardinalityEstimator card;
	
	private OptimizerPlanOperator [] optimizerPlanOperator;

	private long [] cost;
	
	private Map<Integer, Integer> m = new HashMap<Integer, Integer>();
	
	private Map<Relation, Integer> hm = new HashMap<Relation, Integer>();
	
	public JoinOrderOptimizerImpl(CardinalityEstimator ce) {
		this.card = ce;
		
	}
	
	private int power2(int x) {
		int tmp = 1;
		for (int i = 0; i < x; i++)
			tmp = tmp * 2;
		return tmp;
	}
	
	private JoinPredicate check(int left, int right, Relation[] relations, JoinGraphEdge[] joins) {
		HashSet<Integer> l = new HashSet<Integer>();
		HashSet<Integer> r = new HashSet<Integer>();
		int k = 0;
		JoinPredicateConjunct ans = new JoinPredicateConjunct();
		while (left > 0 || right > 0) {
			if (left % 2 == 1 && right % 2 == 1)
				return null;
			else if (left % 2 == 1)
				l.add(power2(k));
			else if (right % 2 == 1)
				r.add(power2(k));
			left = left / 2;
			right = right / 2;
			k++;
		}
		boolean flag = false;
		for (int i = 0; i < joins.length; i++) {
			if (l.contains(hm.get(joins[i].getLeftNode())) && r.contains(hm.get(joins[i].getRightNode()))) {
				ans.addJoinPredicate(joins[i].getJoinPredicate());
				flag = true;
			}
			if (r.contains(hm.get(joins[i].getLeftNode())) && l.contains(hm.get(joins[i].getRightNode()))) {
				ans.addJoinPredicate(joins[i].getJoinPredicate().createSideSwitchedCopy());
				flag = true;
			}
		}
		if (flag) {
			if (ans.getConjunctiveFactors().size() == 1)
				return ans.getConjunctiveFactors().get(0);
			else
				return ans;
		}
		else
			return null;
	}
	
	@Override
	public OptimizerPlanOperator findBestJoinOrder(Relation[] relations,
			JoinGraphEdge[] joins) {
	
		int n = power2(relations.length);
		this.optimizerPlanOperator = new OptimizerPlanOperator[n];
		this.cost = new long[n];
		for (int i = 0; i < relations.length; i++) {
			m.put(relations[i].getID(), power2(i));
			hm.put(relations[i], power2(i));
			if (relations[i].getCumulativeCosts() == -1) {
				relations[i].setCumulativeCosts(relations[i].getOutputCardinality());
			}
		}
		
		for (int i = 0; i < n; i++)
			cost[i] = -1;
		
		int id = 1;
		for (int i = 0; i < relations.length; i++) {
			optimizerPlanOperator[id] = relations[i];
			cost[id] = relations[i].getOutputCardinality();
			id = id * 2;
		}
		for (int i = 0; i < relations.length - 1; i++) {
			for (int j = n - 1; j > 0; j--) 
				for (int k = 1; k < n - j; k++){
				if (cost[j] != -1 && cost[k] != -1 && check(j, k, relations, joins) != null) {
					AbstractJoinPlanOperator tmp = new AbstractJoinPlanOperator(optimizerPlanOperator[j], optimizerPlanOperator[k],check(j, k, relations, joins));
					card.estimateJoinCardinality(tmp);
					tmp.setCumulativeCosts(cost[j] + cost[k] + tmp.getOutputCardinality());
					if (cost[j] + cost[k] + tmp.getOutputCardinality() <= cost[j + k] || cost[j + k] == -1) {
						optimizerPlanOperator[j + k] = tmp;
						cost[j + k] = cost[j] + cost[k] + tmp.getOutputCardinality();
					}
				}
			}
		}
		return optimizerPlanOperator[n - 1];
	}

}
