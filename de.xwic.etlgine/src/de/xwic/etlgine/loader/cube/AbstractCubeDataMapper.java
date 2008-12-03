/*
 * de.xwic.etlgine.loader.cube.DefaultCubeDataMapper 
 */
package de.xwic.etlgine.loader.cube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLContext;
import de.xwic.etlgine.IRecord;

/**
 * Default mapping implementation.
 * @author lippisch
 */
public abstract class AbstractCubeDataMapper implements ICubeDataMapper {

	protected ICube cube = null;
	protected Map<IDimension, DimensionMapping> dimMap = new HashMap<IDimension, DimensionMapping>();
	
	protected List<IMeasure> measures = new ArrayList<IMeasure>();
	protected Map<IMeasure, MeasureMapping> measureMap = new HashMap<IMeasure, MeasureMapping>();
	
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#initialize(de.xwic.etlgine.IETLContext, de.xwic.cube.ICube)
	 */
	public void initialize(IETLContext context, ICube cube) throws ETLException {
		this.cube = cube;
		configure(context);
		
		// notify mappings
		for (DimensionMapping dm : dimMap.values()) {
			dm.afterConfiguration(context, cube);
		}
		for (MeasureMapping mm : measureMap.values()) {
			mm.afterConfiguration(context, cube);
		}
	}
	
	/**
	 * Implementors must setup the configuration here.
	 * @param context
	 * @throws ETLException 
	 */
	protected abstract void configure(IETLContext context) throws ETLException;
	
	/**
	 * Add a mapping where the columnName and dimension name is the same.
	 * @param columnName
	 * @return
	 */
	public DimensionMapping addMapping(String dimKey) {
		return addMapping(dimKey, dimKey);
	}
	/**
	 * Add a mapping for the specified dimension key and column name.
	 * @param dimKey
	 * @param columnName
	 * @return
	 */
	public DimensionMapping addMapping(String dimKey, String... columnNames) {
		
		IDimension dim = cube.getDataPool().getDimension(dimKey);
		DimensionMapping dm = new DimensionMapping(dim);
		dm.setColumnNames(columnNames);
		dimMap.put(dim, dm);
		return dm;
	}
	
	/**
	 * Add a measure to the mapping.
	 * @param measureKey
	 * @param columnName
	 * @return
	 */
	public MeasureMapping addMeasure(String measureKey, String columnName) {
		IMeasure measure = cube.getDataPool().getMeasure(measureKey);
		MeasureMapping mm = new MeasureMapping();
		mm.setColumnName(columnName);
		
		measureMap.put(measure, mm);
		measures.add(measure);
		return mm;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getElement(de.xwic.cube.IDimension, de.xwic.etlgine.IRecord)
	 */
	public IDimensionElement getElement(IDimension dim, IRecord record) throws ETLException {
		DimensionMapping dm = dimMap.get(dim);
		return dm.mapElement(cube, record);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getMeasures()
	 */
	public List<IMeasure> getMeasures() {
		return measures;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getValue(de.xwic.cube.IMeasure, de.xwic.etlgine.IRecord)
	 */
	public Double getValue(IMeasure measure, IRecord record) throws ETLException {
		MeasureMapping mm = measureMap.get(measure);
		return mm.getValue(cube, record);
	}

}
