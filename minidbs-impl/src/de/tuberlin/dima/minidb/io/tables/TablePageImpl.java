package de.tuberlin.dima.minidb.io.tables;

import java.util.LinkedList;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.PageExpiredException;
import de.tuberlin.dima.minidb.io.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;

/**
 * Exercise 1
 * new added implementation for interface TablePage
 * To be modified
 * @author Titicaca
 *
 */
public class TablePageImpl implements TablePage{

	private TableSchema schema;
	private byte[] binPage;
	
	private int pageNumber;
	private int numRecords;
	private int recordWidth;
	private int chunkOffset;
	
	private boolean isExpired;
	private boolean isModified;
	

	
	public TablePageImpl(TableSchema schema, byte[] binPage) throws PageFormatException {
		//TODO:check binPage.length == schema.getPageSize() ??
		this.schema = schema; 
		this.binPage = binPage;
		
		parseBinaryPageHeader();
		
		this.isExpired = false;
		this.isModified = false;
	}
	
	public TablePageImpl(TableSchema tableSchema, byte [] binpage, int pageNum) throws PageFormatException{
		//TODO:check binPage.length == schema.getPageSize() ??
		this.schema = tableSchema;
		this.binPage = binpage;
		
		this.pageNumber = pageNum;
		this.numRecords = 0;
		this.chunkOffset = schema.getPageSize().getNumberOfBytes();
		this.recordWidth = 0;
		
		for(int i=0; i<schema.getNumberOfColumns(); ++i) {
			DataType dataType = schema.getColumn(i).getDataType();
			if(dataType.isArrayType()) {
				if(dataType.isFixLength()) {
					this.recordWidth += dataType.getLength()*dataType.getNumberOfBytes();
				}
				else {
					this.recordWidth += 8;
				}
			}
			else {
				this.recordWidth += dataType.getNumberOfBytes();
			}
		}
		
		IntField.encodeIntAsBinary(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER, binPage, 0);
		IntField.encodeIntAsBinary(pageNumber, binpage, 4);
		IntField.encodeIntAsBinary(numRecords, binpage, 8);
		IntField.encodeIntAsBinary(recordWidth, binpage, 12);
		IntField.encodeIntAsBinary(chunkOffset, binpage, 16);
		
		this.isExpired = false;
		this.isModified = false;
	}
	
	private void parseBinaryPageHeader() throws PageFormatException {
		if(binPage == null) {
			throw new PageFormatException("Null Binary Page!");
		}
		else {
			if(binPage.length < TABLE_DATA_PAGE_HEADER_BYTES)
				throw new PageFormatException("Invalid Page Header Length "+binPage.length);
			
			int magicNumber = IntField.getIntFromBinary(binPage, 0);
			if(magicNumber != TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER) 
				throw new PageFormatException("Invalid Page Header Magic Number "+magicNumber);
			
			this.pageNumber = IntField.getIntFromBinary(binPage, 4);
			this.numRecords = IntField.getIntFromBinary(binPage, 8);
			this.recordWidth = IntField.getIntFromBinary(binPage, 12);
			this.chunkOffset = IntField.getIntFromBinary(binPage, 16);
		}
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
		return this.binPage;
	}

	@Override
	public int getPageNumber() throws PageExpiredException {
		if(isExpired) throw new PageExpiredException();
		else return pageNumber;
	}

	@Override
	public int getNumRecordsOnPage() throws PageExpiredException {
		if(isExpired) throw new PageExpiredException();
		else return numRecords;
	}
	
	public static void encodeBigIntAsBinary(long value, byte[] buffer, int offset)
    {
	    buffer[offset]     = (byte) value;
	    buffer[offset + 1] = (byte) (value >>> 8);
	    buffer[offset + 2] = (byte) (value >>> 16);
	    buffer[offset + 3] = (byte) (value >>> 24);
	    buffer[offset + 4] = (byte) (value >>> 32);
	    buffer[offset + 5] = (byte) (value >>> 40);
	    buffer[offset + 6] = (byte) (value >>> 48);
	    buffer[offset + 7] = (byte) (value >>> 56);
    }
 
	@Override
	public boolean insertTuple(DataTuple tuple) throws PageFormatException,
			PageExpiredException {
		
		//TODO: check tombstone and overwrite???

		if(this.isExpired)
			throw new PageExpiredException();
		
		int recordsEndPos = numRecords*recordWidth + TABLE_DATA_PAGE_HEADER_BYTES;
		if(this.chunkOffset <= recordsEndPos) 
			throw new PageFormatException();


		byte[] record = new byte[this.recordWidth];
		IntField.encodeIntAsBinary(0, record, 0);	// set tombstone false 
		int recordOffset = 4;	// metadata offset

		int newChunkOffset = this.chunkOffset;
		List<byte[]> varList = new LinkedList<byte[]>();
		
		//TODO: not sure if like this
		for(int i = 0; (i < tuple.getNumberOfFields()); ++i) {
			DataField field = tuple.getField(i);
			BasicType type = field.getBasicType();
			
			if(type.isArrayType()) {
				if(type.isFixLength()) {
					//fixed length array, store the whole value
					recordOffset += field.encodeBinary(record, recordOffset);
				}
				else {
					//variable length array
					int fieldLen = field.getNumberOfBytes();
					newChunkOffset -= fieldLen;
					
					//TODO: encode big in !!!
					long pointer = (fieldLen << 32) | newChunkOffset;	 
					encodeBigIntAsBinary(pointer, record, recordOffset);
					recordOffset += 8;
					
					byte[] encoded = new byte[fieldLen];
					field.encodeBinary(encoded, 0);
					varList.add(encoded);
				}
			}
			else {
				//fixed length non-array value
				recordOffset += field.encodeBinary(record, recordOffset);
			}
		}
		if(newChunkOffset < (recordsEndPos+recordWidth)) {
			return false;	// not enough space
		}
		else {
			//do the actual insertion
			System.arraycopy(record, 0, binPage, recordsEndPos, record.length);
			for(byte[] b : varList) {
				this.chunkOffset -= b.length;
				System.arraycopy(b, 0, binPage, this.chunkOffset, b.length);
			}
			this.numRecords ++;
			this.isModified = true;
		}
		return true;
	}

	@Override
	public void deleteTuple(int position) throws PageTupleAccessException,
			PageExpiredException {
		
		if(this.isExpired)
			throw new PageExpiredException();
		
		if(position >= this.numRecords)
			throw new PageTupleAccessException(position);
		

		int recordOffset = TABLE_DATA_PAGE_HEADER_BYTES + position*recordWidth;
		
		int metadata = IntField.getIntFromBinary(binPage, recordOffset);
		metadata &= 0xfffffffe;
		IntField.encodeIntAsBinary(metadata, binPage, recordOffset);
		
		//TODO: remove variable length data??
		
		this.numRecords--;
		this.isModified = true;
	}
	
	@Override
	public DataTuple getDataTuple(int position, long columnBitmap, int numCols)
			throws PageTupleAccessException, PageExpiredException {
		
		if(this.isExpired)
			throw new PageExpiredException();
		
		if(position >= this.numRecords)
			throw new PageTupleAccessException(position);
		
		int recordOffset = TABLE_DATA_PAGE_HEADER_BYTES + position*recordWidth;
		
		//TODO: direct visit byte if endian known
		int metadata = IntField.getIntFromBinary(binPage, recordOffset);
		if((metadata & 0x01) == 1) {
			return null;
		}
		
		if(numCols > schema.getNumberOfColumns()) {
			//TODO: what to do with this situation ??
			return null;
		}
		
		DataTuple result = new DataTuple(numCols);
		
		recordOffset += 4;	//skip metadata
		
		int addedCols = 0;
		int schemaColIndex = 0;
		for ( ; addedCols < numCols && (columnBitmap != 0) && (schemaColIndex < schema.getNumberOfColumns()); columnBitmap >>>= 1, schemaColIndex++) {
			if ((columnBitmap & 0x1) == 0) {
				continue;
			}
			
			DataType dataType = this.schema.getColumn(schemaColIndex).getDataType();
			DataField field = null;
			
			if(dataType.isArrayType()) {
				if(dataType.isFixLength()) {
					//TODO: treat as fixed or variable ?? now as fixed
					int len = dataType.getNumberOfBytes() * dataType.getLength();
					
					field = dataType.getFromBinary(binPage, recordOffset, len);
					recordOffset += len;

				}
				else {
					//variable length value, 8 byte pointer
					BigIntField pointer = BigIntField.getFieldFromBinary(binPage, recordOffset);
					int low = (int) (pointer.getValue() & 0x00000000ffffffff);	//offset to field on the page
					int high = (int) (pointer.getValue() >>> 32);				//length of the field
					
					field = dataType.getFromBinary(binPage, low, high);
					recordOffset += 8;
				}
			}
			else {
				//a fixed length value
				field = dataType.getFromBinary(binPage, recordOffset);
				recordOffset += dataType.getNumberOfBytes();
			}

			//TODO: like this or the same as column bitmap ??
			result.assignDataField(field, addedCols);
			addedCols++;
		}
		
		return result;
	}
	

	@Override
	public DataTuple getDataTuple(LowLevelPredicate[] preds, int position,
			long columnBitmap, int numCols) throws PageTupleAccessException,
			PageExpiredException {
		
		DataTuple tuple = this.getDataTuple(position, columnBitmap, numCols);
		
		if(tuple == null)
			return null;
		
		try {
			for (LowLevelPredicate pred : preds) {
				if (!pred.evaluate(tuple)) 
					return null;
			}
		} catch (QueryExecutionException e) {
			return null;
		}
		
		return tuple;
	}

	@Override
	public TupleIterator getIterator(int numCols, long columnBitmap)
			throws PageTupleAccessException, PageExpiredException {
		DataTuple [] tuples = new DataTuple[this.numRecords];
		for(int i = 0; i < this.numRecords; i ++){
			tuples[i] = this.getDataTuple(i, columnBitmap, numCols); //tuples[i] might be null
		}
		TupleIterator iter = new TupleIteratorImpl(tuples);
		return iter;
	}

	@Override
	public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols,
			long columnBitmap) throws PageTupleAccessException,
			PageExpiredException {
		DataTuple [] tuples = new DataTuple[this.numRecords];
		for(int i = 0; i < this.numRecords; i ++){
			tuples[i] = this.getDataTuple(preds, i, columnBitmap, numCols); //tuples[i] might be null
		}
		TupleIterator iter = new TupleIteratorImpl(tuples);
		return iter;
	}

	@Override
	public TupleRIDIterator getIteratorWithRID()
			throws PageTupleAccessException, PageExpiredException {
		DataTuple [] tuples = new DataTuple[this.numRecords];
		RID []  rids = new RID[this.numRecords];
		for(int i = 0; i < this.numRecords; i ++){
			tuples[i] = this.getDataTuple(i, Long.MAX_VALUE, schema.getNumberOfColumns()); //tuples[i] might be null
			rids[i] = new RID(this.pageNumber, i);			
		}
		TupleRIDIterator iter = new TupleRIDIteratorImpl(tuples,rids);
		return iter;
	}
	
}