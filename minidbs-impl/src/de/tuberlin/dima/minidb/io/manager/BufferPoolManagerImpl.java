package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class BufferPoolManagerImpl implements BufferPoolManager{

	@Override
	public void startIOThreads() throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeBufferPool() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerResource(int id, ResourceManager manager)
			throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheableData unpinAndGetPageAndPin(int resourceId,
			int unpinPageNumber, int getPageNumber) throws BufferPoolException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prefetchPage(int resourceId, int pageNumber)
			throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prefetchPages(int resourceId, int startPageNumber,
			int endPageNumber) throws BufferPoolException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId, Enum<?> type)
			throws BufferPoolException, IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
}