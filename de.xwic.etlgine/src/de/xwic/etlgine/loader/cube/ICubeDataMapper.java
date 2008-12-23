/*
 * de.xwic.etlgine.loader.cube.CubeDataMapper 
 */
package de.xwic.etlgine.loader.cube;

import java.util.List;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IRecord;

/**
 * Used to map record values to dimensions. 
 * @author Lippisch
 */
public interface ICubeDataMapper {

	/**
	 * Initialize the mapper.
	 * @param context
	 * @param cube
	 * @throws ETLException 
	 */
	public void initialize(IContext context, ICube cube) throws ETLException;
	
	/**
	 * Returns the target element for the specified dimension.
	 * @param dim
	 * @param record
	 * @return
	 * @throws ETLException 
	 */
	public IDimensionElement getElement(IDimension dim, IRecord record) throws ETLException;

	/**
	 * Returns the measures that are mapped.
	 * @return
	 */
	public List<IMeasure> getMeasures();

	/**
	 * Returns the value. If the returned value is NULL, the data is not
	 * written into the cube.
	 * @param measure
	 * @param record
	 * @return
	 * @throws ETLException 
	 */
	public Double getValue(IMeasure measure, IRecord record) throws ETLException;
	
	
	

}
