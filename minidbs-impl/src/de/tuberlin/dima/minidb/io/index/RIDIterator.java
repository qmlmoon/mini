package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeLeafPage;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class RIDIterator implements IndexResultIterator<RID> {
	
	BTreeIndexImpl btree;
	int nextLeaf;
	
	List<RID> results;
	DataField startKey;
	DataField stopKey = null;
	boolean startKeyIncluded = false;
	boolean stopKeyIncluded = false;
	
	int cursor = 0;
	boolean hasNextPage = true;
	
	public RIDIterator() {
		this.hasNextPage = false;
	}
	
	public RIDIterator(BTreeIndexImpl btree, int leafPageNum, DataField key) {
		this.btree = btree;
		this.nextLeaf = leafPageNum;
		
		this.results = new ArrayList<RID>();
		
		this.startKey = key;
		this.stopKey = null;
	}
	
	public RIDIterator(BTreeIndexImpl btree, int leafPageNum, DataField start, DataField stop, boolean startKeyIncluded, boolean stopKeyIncluded) {
		this.btree = btree;
		this.nextLeaf = leafPageNum;
		
		this.results = new ArrayList<RID>();
		
		this.startKey = start;
		this.stopKey = stop;
		this.startKeyIncluded = startKeyIncluded;
		this.stopKeyIncluded = stopKeyIncluded;
	}

	@Override
	public boolean hasNext() throws IOException, IndexFormatCorruptException,
			PageFormatException {
		if(this.results == null) 
			return false;
		if(cursor == results.size()) {
			if(this.hasNextPage) {
				
				BTreeLeafPage leaf = this.btree.getLeafPage(this.nextLeaf);
				if(leaf == null) {
					this.hasNextPage = false;
					return false;
				}
				
				if(this.stopKey == null) {
					//iterator for single key
					this.hasNextPage = false;
					if(leaf.getAllsRIDsForKey(this.startKey, this.results)) {
						this.hasNextPage = leaf.isLastKeyContinuingOnNextPage();
					}
				}
				else {
					//iterator for range key
					int pos = 0;
					if(leaf.getFirstKey().compareTo(startKey) < 0) {
						pos = leaf.getPositionForKey(startKey);
						if(!this.startKeyIncluded) {
							pos++;
							while(pos<leaf.getNumberOfEntries() && leaf.getKey(pos).compareTo(startKey)==0) {
								pos++;
							}
						}
					}
					//now pos is the position of first matched value
					while(pos<leaf.getNumberOfEntries()) {
						int compareToStopKey = leaf.getKey(pos).compareTo(stopKey);
						
						if((compareToStopKey>0) || (compareToStopKey==0 && !this.stopKeyIncluded)) {
							this.hasNextPage = false;
							break;
						}
						
						this.results.add(leaf.getRidAtPosition(pos));
						pos++;
					}
					//check whether next page still contain matched values
					this.hasNextPage = false;
					if(pos == leaf.getNumberOfEntries()) {
						int compareToStopKey = leaf.getLastKey().compareTo(stopKey);
						if(compareToStopKey==0 && this.stopKeyIncluded) {
							this.hasNextPage = leaf.isLastKeyContinuingOnNextPage();
						}
						else if(compareToStopKey < 0) {
							this.hasNextPage = true;
						}
					}
				}

				//TODO: need unpin page??
				this.btree.unpinPage(this.nextLeaf);
				
				//update next leaf page number
				this.nextLeaf = leaf.getNextLeafPageNumber();
				if(this.nextLeaf == -1) {
					this.hasNextPage = false;
				}
			}
		}
		return this.cursor < this.results.size();

	}

	@Override
	public RID next() throws IOException, IndexFormatCorruptException,
			PageFormatException {
		if(this.hasNext()) {
			RID result = this.results.get(cursor);
			cursor++;
			return result;
		}
		return null;
	}
}
