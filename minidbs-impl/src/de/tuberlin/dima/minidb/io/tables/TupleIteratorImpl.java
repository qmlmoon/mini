package de.tuberlin.dima.minidb.io.tables;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataTuple;

public class TupleIteratorImpl implements TupleIterator{
	
	protected List<DataTuple> list = new ArrayList<DataTuple>();
	private int index;

	TupleIteratorImpl(DataTuple [] tuples){
		index = -1;
		for(int i = 0; i < tuples.length; i++){
			list.add(tuples[i]);
		}
	}
	
	@Override
	public boolean hasNext() throws PageTupleAccessException {
		if((index +1) < list.size()){
			return true;			
		}else{			
			return false;
		}
	}

	@Override
	public DataTuple next() throws PageTupleAccessException {
		index++;
		return list.get(index);
	}
	
}