/*
 * de.xwic.etlgine.loader.cube.IDataPoolProvider 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.IDataPool;

/**
 * Provides access to the target data pool.
 * @author lippisch
 */
public interface IDataPoolProvider {

	/**
	 * Returns the data pool.
	 * @return
	 */
	public IDataPool getDataPool();
	
}
