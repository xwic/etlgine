/*
 * de.xwic.etlgine.loader.cube.IDataPoolProvider 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.IDataPool;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;

/**
 * Provides access to the target data pool.
 * @author lippisch
 */
public interface IDataPoolProvider {

	/**
	 * Returns the data pool.
	 * @return
	 */
	public IDataPool getDataPool(IContext context) throws ETLException;
	
}
