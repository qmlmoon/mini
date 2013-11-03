package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.PageExpiredException;
import de.tuberlin.dima.minidb.io.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

/**
 * Exercise 1
 * new added implementation for interface TablePage
 * To be modified
 * @author Titicaca
 *
 */
public class TablePageImpl implements TablePage{
	
	private int pageNumber;
	private int recordsNum;
	private int recordWidth;
	private int chunkOffset;
	private boolean isExpired;
	private boolean isModified;
	private byte[] binPage;
	private TableSchema schema;
	
	public TablePageImpl(TableSchema tableSchema, byte [] binpage, int pageNum) throws PageFormatException{
		pageNumber = pageNum;
		schema = tableSchema;
		binPage = binpage;
		pageNumber = pageNum;
		isModified = false;
		recordsNum = 0;
		//TODO if the header was found to be corrupt. Throw PageFormatException

	}

	@Override
	public boolean hasBeenModified() throws PageExpiredException {
		if(isExpired) throw new PageExpiredException();		
		else return isModified;
	}

	@Override
	public void markExpired() {
		isExpired = true;
	}

	@Override
	public boolean isExpired() {
		return isExpired;
	}

	@Override
	public byte[] getBuffer() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPageNumber() throws PageExpiredException {
		if(isExpired) throw new PageExpiredException();
		else return pageNumber;
	}

	@Override
	public int getNumRecordsOnPage() throws PageExpiredException {
		if(isExpired) throw new PageExpiredException();
		else return recordsNum;
	}
 
	@Override
	public boolean insertTuple(DataTuple tuple) throws PageFormatException,
			PageExpiredException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void deleteTuple(int position) throws PageTupleAccessException,
			PageExpiredException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataTuple getDataTuple(int position, long columnBitmap, int numCols)
			throws PageTupleAccessException, PageExpiredException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataTuple getDataTuple(LowLevelPredicate[] preds, int position,
			long columnBitmap, int numCols) throws PageTupleAccessException,
			PageExpiredException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleIterator getIterator(int numCols, long columnBitmap)
			throws PageTupleAccessException, PageExpiredException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols,
			long columnBitmap) throws PageTupleAccessException,
			PageExpiredException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleRIDIterator getIteratorWithRID()
			throws PageTupleAccessException, PageExpiredException {
		// TODO Auto-generated method stub
		return null;
	}
	
}