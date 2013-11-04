package de.tuberlin.dima.minidb.io.tables;

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
	
	private final int VARCHAR_NULL_LENGTH = -1;
	
	public TablePageImpl(TableSchema schema, byte[] binPage) throws PageFormatException {
		//TODO:check binPage.length == schema.getPageSize() ??
		this.schema = schema; 
		this.binPage = binPage;
		
		parseBinaryPageHeader();
		
		if(this.recordWidth != calcuRecordWidth(schema))
			throw new PageFormatException("Invalid record width "+this.recordWidth);
		
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
		this.recordWidth = calcuRecordWidth(schema);
		
		IntField.encodeIntAsBinary(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER, binPage, 0);
		IntField.encodeIntAsBinary(pageNumber, binPage, 4);
		IntField.encodeIntAsBinary(numRecords, binPage, 8);
		IntField.encodeIntAsBinary(recordWidth, binPage, 12);
		IntField.encodeIntAsBinary(chunkOffset, binPage, 16);
		
		this.isExpired = false;
		this.isModified = true;
		
	}
	
	private int calcuRecordWidth(TableSchema schema) {
		int result = 4;
		for(int i=0; i<schema.getNumberOfColumns(); ++i) {
			DataType dataType = schema.getColumn(i).getDataType();
			if(dataType.isArrayType()) {
				if(dataType.isFixLength()) {
					result += dataType.getNumberOfBytes();
				}
				else {
					result += 8;
				}
			}
			else {
				result += dataType.getNumberOfBytes();
			}
		}
		return result;
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
			
			pageNumber = IntField.getIntFromBinary(binPage, 4);
			numRecords = IntField.getIntFromBinary(binPage, 8);
			recordWidth = IntField.getIntFromBinary(binPage, 12);
			chunkOffset = IntField.getIntFromBinary(binPage, 16);
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
		return binPage;
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
	
	private void updatePageHeader() {
		IntField.encodeIntAsBinary(numRecords, binPage, 8);
		IntField.encodeIntAsBinary(chunkOffset, binPage, 16);
	}
 
	@Override
	public boolean insertTuple(DataTuple tuple) throws PageFormatException,
			PageExpiredException {
		
		//TODO: check tombstone and overwrite???

		if(isExpired)
			throw new PageExpiredException();
		
		int recordsEndPos = numRecords*recordWidth + TABLE_DATA_PAGE_HEADER_BYTES;
		if(chunkOffset < recordsEndPos) 
			throw new PageFormatException();
		
		DataField field = null;
		DataType dataType = null;
		
		int totalLength = recordWidth;
		for(int i=0; (i < tuple.getNumberOfFields()) && (i < schema.getNumberOfColumns()); ++i) {
			field = tuple.getField(i);
			dataType = schema.getColumn(i).getDataType();
			if(dataType.isArrayType() && (!dataType.isFixLength())) {
				totalLength += field.getNumberOfBytes();
			}
		}
		
		if(totalLength > (chunkOffset-recordsEndPos)) {
			System.out.println("No enougn space for inserting new tuple "+tuple.toString());
			return false;	// not enough space
		}

		int recordOffset = recordsEndPos;
		
		//set metadata, tombstone as false
		IntField.encodeIntAsBinary(0, binPage, recordOffset);
		recordOffset += 4;

		//TODO: not sure if like this
		for(int i = 0; (i < tuple.getNumberOfFields()) && (i < schema.getNumberOfColumns()); ++i) {
			field = tuple.getField(i);
			dataType = schema.getColumn(i).getDataType();
			
			System.out.println("Insert "+field.getNumberOfBytes()+" bytes in recordOffset "+recordOffset+" with value "+field.toString());
			if(dataType.isArrayType()) {
				if(dataType.isFixLength()) {
					//fixed length array, store the whole value
					field.encodeBinary(binPage, recordOffset);
					recordOffset += dataType.getNumberOfBytes();
				}
				else {
					//variable length array
					int fieldLen = field.getNumberOfBytes();
					chunkOffset -= fieldLen;
					field.encodeBinary(binPage, chunkOffset);
					
					long pointer = 0;
					if(field.isNULL())
						fieldLen = VARCHAR_NULL_LENGTH;
					pointer |= fieldLen;
					pointer <<= 32;
					pointer &= 0xffffffff00000000L;
					pointer |= chunkOffset;	//length of the field is high 32-bit, while offset is low 32-bit 
					encodeBigIntAsBinary(pointer, binPage, recordOffset);
					recordOffset += 8;
					System.out.println("******* in insert tuple : offset is "+chunkOffset+" and length is "+fieldLen);
				}
			}
			else {
				//fixed length non-array value
				recordOffset += field.encodeBinary(binPage, recordOffset);
			}
		}
		
		numRecords ++;
		System.out.println("new record "+numRecords+" Inserted +++++++++++++++++++++++++++++++++++++++++++++");
		isModified = true;
		updatePageHeader();
//		System.out.println("Inserted Tuple "+tuple.toString());
		return true;
	}

	@Override
	public void deleteTuple(int position) throws PageTupleAccessException,
			PageExpiredException {
		
		if(isExpired)
			throw new PageExpiredException();
		
		if(position >= numRecords)
			throw new PageTupleAccessException(position);
		

		int recordOffset = TABLE_DATA_PAGE_HEADER_BYTES + position*recordWidth;
		
		int metadata = IntField.getIntFromBinary(binPage, recordOffset);
		metadata &= 0xfffffffe;
		IntField.encodeIntAsBinary(metadata, binPage, recordOffset);
		
		//TODO: remove variable length data??
		
		numRecords--;
		isModified = true;
		updatePageHeader();
	}
	
	@Override
	public DataTuple getDataTuple(int position, long columnBitmap, int numCols)
			throws PageTupleAccessException, PageExpiredException {
		
		if(isExpired)
			throw new PageExpiredException();
		
		if(position >= numRecords)
			throw new PageTupleAccessException(position);
		
		int recordOffset = TABLE_DATA_PAGE_HEADER_BYTES + position*recordWidth;
		System.out.println("Fetch record "+position+" ================================");
		//TODO: direct visit byte if endian known
		int metadata = IntField.getIntFromBinary(binPage, recordOffset);
		if((metadata & 0x01) == 1) {
			System.out.println("Tome Stone is true for record "+position);
			return null;
		}
		recordOffset += 4;	//skip metadata
		
		if(numCols > schema.getNumberOfColumns()) {
			//TODO: what to do with this situation ??
			System.out.println("required number of columns is larger than total columns in Table schema!");
			return null;
		}
		
		DataTuple result = new DataTuple(numCols);
		
		int addedCols = 0;
		int schemaColIndex = 0;
		for ( ; (addedCols < numCols) && (columnBitmap != 0) && (schemaColIndex < schema.getNumberOfColumns()); 
				columnBitmap >>>= 1, schemaColIndex++) {
			if ((columnBitmap & 0x1) == 0) {
				continue;
			}
			
			DataType dataType = schema.getColumn(schemaColIndex).getDataType();
			DataField field = null;
			
			if(dataType.isArrayType()) {
				if(dataType.isFixLength()) {
					//TODO: treat as fixed or variable ?? now as fixed
					int len = dataType.getNumberOfBytes();
					System.out.println("--------------------------- dataType.getNumberOfBytes = "+len);
					field = dataType.getFromBinary(binPage, recordOffset, len);
					recordOffset += len;
				}
				else {
					//variable length value, 8 byte pointer
					BigIntField pointer = BigIntField.getFieldFromBinary(binPage, recordOffset);
					int low = (int) (pointer.getValue() & 0xffffffff);	//offset to field on the page
					int high = (int) (pointer.getValue() >>> 32);				//length of the field
					
					System.out.println("+++++++++++++++++++++++++++++++++++++ in get tuple : offset is "+low+" and length is "+high);
					if(high == VARCHAR_NULL_LENGTH) {
						field = dataType.getNullValue();
					}
					else {
						field = dataType.getFromBinary(binPage, low, high);
					}
					recordOffset += 8;
				}
			}
			else {
				//a fixed length value
				field = dataType.getFromBinary(binPage, recordOffset);
				recordOffset += field.getNumberOfBytes();
			}
			System.out.println("fetch "+field.getNumberOfBytes()+" bytes before recordOffset "+recordOffset+" with value "+field.toString());

			//TODO: like this or the same as column bitmap ??
			result.assignDataField(field, addedCols);
			addedCols++;
		}
//		System.out.println("Get tuple "+result.toString());
		
		return result;
	}
	

	@Override
	public DataTuple getDataTuple(LowLevelPredicate[] preds, int position,
			long columnBitmap, int numCols) throws PageTupleAccessException,
			PageExpiredException {
		
		DataTuple tuple = getDataTuple(position, columnBitmap, numCols);
		
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
		DataTuple [] tuples = new DataTuple[numRecords];
		for(int i = 0; i < numRecords; i ++){
			tuples[i] = getDataTuple(i, columnBitmap, numCols); //tuples[i] might be null
		}
		TupleIterator iter = new TupleIteratorImpl(tuples);
		return iter;
	}

	@Override
	public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols,
			long columnBitmap) throws PageTupleAccessException,
			PageExpiredException {
		DataTuple [] tuples = new DataTuple[numRecords];
		for(int i = 0; i < numRecords; i ++){
			tuples[i] = getDataTuple(preds, i, columnBitmap, numCols); //tuples[i] might be null
		}
		TupleIterator iter = new TupleIteratorImpl(tuples);
		return iter;
	}

	@Override
	public TupleRIDIterator getIteratorWithRID()
			throws PageTupleAccessException, PageExpiredException {
		DataTuple [] tuples = new DataTuple[numRecords];
		RID []  rids = new RID[numRecords];
		for(int i = 0; i < numRecords; i ++){
			tuples[i] = getDataTuple(i, Long.MAX_VALUE, schema.getNumberOfColumns()); //tuples[i] might be null
			rids[i] = new RID(pageNumber, i);			
		}
		TupleRIDIterator iter = new TupleRIDIteratorImpl(tuples,rids);
		return iter;
	}
/*	
	public static void main(String[] args) {
		DataTuple tuple = new DataTuple(5);
		DataField field = null;
		
		field = new FloatField((float) 0.044571638);
		tuple.assignDataField(field, 0);
		
		field = new IntField(-589534095);
		tuple.assignDataField(field, 1);
		
		field = new FloatField((float) 0.9556602);
		tuple.assignDataField(field, 2);
		
		field = new FloatField((float) 0.27060378);
		tuple.assignDataField(field, 3);
		
		field = new SmallIntField((short) -32036);
		tuple.assignDataField(field, 4);
		
		byte[] bin = new byte[PageSize.getDefaultPageSize().getNumberOfBytes()];
		TableSchema sm = new TableSchema();
		sm.addColumn(col);
		TablePage page = new TablePage();
		
	}
	*/
}
