package de.tuberlin.dima.minidb.io.tables;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.util.Pair;

public class TupleRIDIteratorImpl implements TupleRIDIterator{

	protected List<DataTuple> list = new ArrayList<DataTuple>();
	protected List<RID> ridList = new ArrayList<RID>();
	private int length;
	private int index;
	
	public TupleRIDIteratorImpl(DataTuple [] tuples , RID [] rids){
		index = -1;
		length = Math.min(tuples.length, rids.length);
		for (int i = 0; i < length; i++){
			list.add(tuples[i]);
			ridList.add(rids[i]);
		}
	}
	
	@Override
	public boolean hasNext() throws PageTupleAccessException {
		if((index +1) < length){
			return true;			
		}else{			
			return false;
		}
	}

	@Override
	public Pair<DataTuple, RID> next() throws PageTupleAccessException {
		index++;
		return new Pair<DataTuple, RID>(list.get(index), ridList.get(index));
	}
	
}