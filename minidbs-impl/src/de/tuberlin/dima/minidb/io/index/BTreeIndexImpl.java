package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;

public class BTreeIndexImpl implements BTreeIndex {

	@Override
	public IndexSchema getIndexSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField key)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexResultIterator<RID> lookupRids(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexResultIterator<DataField> lookupKeys(DataField startKey,
			DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
			throws PageFormatException, IndexFormatCorruptException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertEntry(DataField key, RID rid) throws PageFormatException,
			IndexFormatCorruptException, DuplicateException, IOException {
		// TODO Auto-generated method stub

	}

}
