/*
 * de.xwic.etlgine.loader.cube.CubeDataMapper 
 */
package de.xwic.etlgine.loader.cube;

import java.util.List;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.Key;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Used to map record values to dimensions. 
 * @author Lippisch
 */
public interface ICubeDataMapper {

	/**
	 * Initialize the mapper.
	 * @param processContext
	 * @param cube
	 * @throws ETLException 
	 */
	public void initialize(IProcessContext processContext, ICube cube) throws ETLException;
	
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
	 * /
	public Double getValue(IMeasure measure, IRecord record) throws ETLException;
	*/
	
	public MeasureMapping getMeasureMapping(IMeasure measure);
	
	public DimensionMapping getDimensionMapping(IDimension dimension);
	
	/**
	 * Returns true if the record may be loaded into the cube. The default
	 * implementation returns true, but it may be overwritten to implement
	 * a customized filter.
	 * @param record
	 * @return
	 * @throws ETLException
	 */
	public boolean accept(IRecord record) throws ETLException;

	/**
	 * Clear the cube.
	 * @param cube
	 */
	public void clearCube(ICube cube);

	/**
	 * Invoked right before ICube.addCellValue is called.
	 * @param key
	 * @param measure
	 * @param value
	 * @param record
	 * @throws ETLException
	 */
	public void onAddCellValue(Key key, IMeasure measure, Double value, IRecord record) throws ETLException;

	/**
	 * @return the enforceDimensionMapping
	 */
	public boolean isEnforceDimensionMapping();

	/**
	 * After the cell had been loaded with values, all ICellLoader objects are invoked here.
	 * @param key
	 * @param record
	 * @throws ETLException 
	 */
	public void onCellProcessed(Key key, IRecord record) throws ETLException;

}
