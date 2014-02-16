package de.tuberlin.dima.minidb.core;


import java.util.Arrays;


/**
 * Stub class for a tuple that flows through the DBMS during query execution.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class DataTuple
{
	
	/**
	 * The fields in this tuple.
	 */
	private DataField[] fields;
	
	
	/**
	 * Creates a new empty tuple that may hold the given number of fields.
	 * 
	 * @param numFields The number of fields for the tuple.
	 */
	public DataTuple(int numFields)
	{
		this.fields = new DataField[numFields];
	}
	
	/**
	 * Creates a new tuple that copies the fields from the given array.
	 * 
	 * @param fields The fields for the tuple.
	 */
	public DataTuple(DataField[] fields)
	{
		this.fields = new DataField[fields.length];
		System.arraycopy(fields, 0, this.fields, 0, fields.length);
	}
	
	
	/**
	 * Gets the number of fields in the tuple.
	 * 
	 * @return The number of fields in the tuple.
	 */
	public int getNumberOfFields()
	{
		return this.fields.length;
	}
	
	/**
	 * Gets a field from the tuple.
	 * 
	 * @param pos The position of the field to get. The tuple must be wide enough to include
	 *            this position.
	 * @return The field from the requested position.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the position is negative or the tuple
	 *                                        is less wide than the position requires.
	 */
	public  DataField getField(int pos)
	{
		return this.fields[pos];
	}
	
	/**
	 * Sets the field at the given position in this tuple to the given field.
	 * The tuple must be wide enough to include the given position.
	 *  
	 * @param field The new field to assign to the given position.
	 * @param pos The position to assign the new field to.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the position is negative or the tuple
	 *                                        is less wide than the position requires.
	 */
	public void assignDataField(DataField field, int pos)
	{
		this.fields[pos] = field;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (o != null && o instanceof DataTuple)
		{
			DataTuple other = (DataTuple) o;
			return Arrays.equals(this.fields, other.fields);
		}
		else {
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return Arrays.hashCode(this.fields);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.fields.length; i++) {
			bld.append(this.fields[i] == null ? "null" : this.fields[i].toString());
			if (i < this.fields.length - 1) {
				bld.append(", ");
			}
		}
		bld.append(')');
		return bld.toString();
	}

}
