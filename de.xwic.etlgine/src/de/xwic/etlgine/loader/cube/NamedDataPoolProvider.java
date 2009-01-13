/**
 * 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.IDataPool;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.cube.CubeHandler;

/**
 * @author lippisch
 *
 */
public class NamedDataPoolProvider implements IDataPoolProvider {

	private String dataPoolName;
	
	/**
	 * @param dataPoolName
	 */
	public NamedDataPoolProvider(String dataPoolName) {
		super();
		this.dataPoolName = dataPoolName;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.IDataPoolProvider#getDataPool()
	 */
	public IDataPool getDataPool(IContext context) throws ETLException {
		return CubeHandler.getCubeHandler(context).openDataPool(dataPoolName);
	}

	/**
	 * @return the dataPoolName
	 */
	public String getDataPoolName() {
		return dataPoolName;
	}

}
