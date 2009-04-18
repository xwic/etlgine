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
import de.xwic.cube.Key;
import de.xwic.cube.util.CountLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Default mapping implementation.
 * @author lippisch
 */
public class BaseCubeDataMapper implements ICubeDataMapper {

	protected ICube cube = null;
	protected Map<IDimension, DimensionMapping> dimMap = new HashMap<IDimension, DimensionMapping>();
	
	protected List<IMeasure> measures = new ArrayList<IMeasure>();
	protected Map<IMeasure, MeasureMapping> measureMap = new HashMap<IMeasure, MeasureMapping>();
	protected IProcessContext processContext;
	
	protected Map<CountLoader, String> countLoaderMap = new HashMap<CountLoader, String>();
	
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#initialize(de.xwic.etlgine.IETLContext, de.xwic.cube.ICube)
	 */
	public void initialize(IProcessContext processContext, ICube cube) throws ETLException {
		this.processContext = processContext;
		this.cube = cube;
		configure(processContext);
		
		// register cell value changed listener (replace existing one)
		for (CountLoader loader: countLoaderMap.keySet()) {
			// remove existing one
			int idx_old = cube.getCubeListeners().indexOf(loader);
			if (idx_old != -1) {
				CountLoader loader_old = (CountLoader)cube.getCubeListeners().get(idx_old);
				cube.getCubeListeners().remove(idx_old);
				loader.configure(loader_old);
			}
			cube.getCubeListeners().add(loader);
		}

		// notify mappings
		for (DimensionMapping dm : dimMap.values()) {
			dm.afterConfiguration(processContext, cube);
		}
		for (MeasureMapping mm : measureMap.values()) {
			mm.afterConfiguration(processContext, cube);
		}
	}
	
	/**
	 * Implementors must setup the configuration here.
	 * @param processContext
	 * @throws ETLException 
	 */
	protected void configure(IProcessContext processContext) throws ETLException {
		
	}
	
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
	
	/**
	 * Add a countLoader for a measure counting column values.
	 * @param measureKey
	 * @param countLoader
	 */
	public void addCountLoader(String measureKey, String columnName, CountLoader countLoader) {
		int measureIndex = cube.getMeasureIndex(cube.getDataPool().getMeasure(measureKey));
		countLoader.setMeasureIndex(measureIndex);
		countLoaderMap.put(countLoader, columnName);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getElement(de.xwic.cube.IDimension, de.xwic.etlgine.IRecord)
	 */
	public IDimensionElement getElement(IDimension dim, IRecord record) throws ETLException {
		DimensionMapping dm = dimMap.get(dim);
		if (dm == null) {
			throw new ETLException("No mapping for dimension " + dim.getKey());
		}
		return dm.mapElement(processContext, cube, record);
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

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#accept(de.xwic.etlgine.IRecord)
	 */
	public boolean accept(IRecord record) throws ETLException {
		return true;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#clearCube(de.xwic.cube.ICube)
	 */
	public void clearCube(ICube cube) {
		cube.clear();
		for (CountLoader loader: countLoaderMap.keySet()) {
			loader.clear();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#onAddCellValue(de.xwic.cube.Key, de.xwic.cube.IMeasure, java.lang.Double, de.xwic.etlgine.IRecord)
	 */
	public void onAddCellValue(Key key, IMeasure measure, Double value, IRecord record) throws ETLException {
		// TODO check if using only one measure makes sense and improves performance
		for (Map.Entry<CountLoader, String> entry : countLoaderMap.entrySet()) {
			CountLoader countLoader = entry.getKey();
			String columnName = entry.getValue();
			Object count = record.getData(columnName);
			countLoader.setCountOn(count);
		}
	}
}
