package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;

public class KeyIterator implements IndexResultIterator<DataField> {
	
	BTreeIndexImpl btree;
	int nextLeaf;
	
	List<DataField> results;
	
	DataField startKey;
	DataField stopKey;
	boolean startKeyIncluded = false;
	boolean stopKeyIncluded = false;
	
	int cursor = 0;
	boolean hasNextPage = true;
	
	public KeyIterator() {
		this.hasNextPage = false;
	}
	
	public KeyIterator(BTreeIndexImpl btree, int leafPageNum, DataField start, DataField stop, boolean startKeyIncluded, boolean stopKeyIncluded) {
		this.btree = btree;
		this.nextLeaf = leafPageNum;
		
		this.results = new ArrayList<DataField>();
		
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
				//iterator for range key, find the right start position first
				boolean getRightPosition = false;
				int pos = -1;
				BTreeLeafPage leaf = null;
				do {
					leaf = this.btree.getLeafPage(this.nextLeaf);
					if(leaf == null) {
						this.hasNextPage = false;
						return false;
					}
					
					pos = leaf.getPositionForKey(startKey);
					if(this.startKeyIncluded)
						getRightPosition = true;
					else {
						while(pos<leaf.getNumberOfEntries() && leaf.getKey(pos).compareTo(startKey)==0) {
							pos++;
						}
						if(pos<leaf.getNumberOfEntries())
							getRightPosition = true;
						else {
							getRightPosition = false;
							this.nextLeaf = leaf.getNextLeafPageNumber();
							if(this.nextLeaf == -1) {
								this.hasNextPage = false;
								return false;
							}
							
							//TODO: unpin or not
							this.btree.unpinPage(leaf.getPageNumber());
						}
					}
				}while(!getRightPosition);
				
				//now pos is the position of first matched value
				while(pos<leaf.getNumberOfEntries()) {
					DataField key = leaf.getKey(pos);
					int compareToStopKey = key.compareTo(stopKey);
					
					if((compareToStopKey>0) || (compareToStopKey==0 && !this.stopKeyIncluded)) {
						this.hasNextPage = false;
						break;
					}
					
					//TODO: allow to add duplicate or not??
					this.results.add(key);
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

				//update next leaf page number
				this.nextLeaf = leaf.getNextLeafPageNumber();
				if(this.nextLeaf == -1) {
					this.hasNextPage = false;
				}
				else {
					if(this.hasNextPage)
						this.btree.prefetchPage(this.nextLeaf);
				}
				
				//TODO: need unpin page??
				this.btree.unpinPage(leaf.getPageNumber());
			}
		}
		
		return this.cursor < this.results.size();

	}

	@Override
	public DataField next() throws IOException, IndexFormatCorruptException,
			PageFormatException {
		if(this.hasNext()) {
			DataField result = this.results.get(cursor);
			cursor++;
			return result;
		}
		return null;
	}

}
