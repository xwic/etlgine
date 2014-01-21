/*
 * de.xwic.etlgine.loader.cube.DefaultCubeDataMapper 
 */
package de.xwic.etlgine.loader.cube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.cube.ICell;
import de.xwic.cube.ICellLoader;
import de.xwic.cube.ICube;
import de.xwic.cube.ICubeListener;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.IMeasureLoader;
import de.xwic.cube.Key;
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
	
	protected Map<IMeasureLoader, String> measureLoaderMap = new HashMap<IMeasureLoader, String>();
	protected Map<String, ICellLoader> cellLoaderMap = new HashMap<String, ICellLoader>();
	
	protected boolean enforceDimensionMapping = true;
	protected boolean skipMissingColumns = false;
	
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#initialize(de.xwic.etlgine.IETLContext, de.xwic.cube.ICube)
	 */
	@Override
	public void initialize(IProcessContext processContext, ICube cube) throws ETLException {
		this.processContext = processContext;
		this.cube = cube;
		configure(processContext);
		// register cell value changed listener (replace existing one)
		for (IMeasureLoader loader: measureLoaderMap.keySet()) {
			// remove existing one
			int idx_old = cube.getCubeListeners().indexOf(loader);
			if (idx_old != -1) {
				IMeasureLoader oldLoader = (IMeasureLoader)cube.getCubeListeners().get(idx_old);
				cube.getCubeListeners().remove(idx_old);
				loader.configure(oldLoader);
			}
			cube.getCubeListeners().add(loader);
		}

		for (ICellLoader cellLoader: cellLoaderMap.values()) {
			// remove existing one
			if (cellLoader instanceof ICubeListener) {
				ICubeListener loader = (ICubeListener)cellLoader;
				int idx_old = cube.getCubeListeners().indexOf(loader);
				if (idx_old != -1) {
					//IMeasureLoader oldLoader = (IMeasureLoader)cube.getCubeListeners().get(idx_old);
					cube.getCubeListeners().remove(idx_old);
				}
				cube.getCubeListeners().add(loader);
			}
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
		dm.setSkipMissingColumns(skipMissingColumns);
		dm.setColumnNames(columnNames);
		dimMap.put(dim, dm);
		return dm;
	}
	
	/**
	 * Add a measure to the mapping that just counts.
	 * @param measureKey
	 * @param columnName
	 * @return
	 */
	public MeasureMapping addMeasure(String measureKey) {
		IMeasure measure = cube.getDataPool().getMeasure(measureKey);
		MeasureMapping mm = new MeasureMapping();
		mm.setFixedValue(1.0d);
		mm.setMeasureIndex(cube.getMeasureIndex(measure));
		
		measureMap.put(measure, mm);
		measures.add(measure);
		return mm;
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
		mm.setMeasureIndex(cube.getMeasureIndex(measure));
		
		measureMap.put(measure, mm);
		measures.add(measure);
		return mm;
	}

	
	/**
	 * Add a IMeasureLoader for customer measure setting other than default sum aggregation.
	 * @param measureKey
	 * @param columnName
	 * @param loader
	 */
	public void addMeasure(String measureKey, String columnName, IMeasureLoader loader) {
		if (loader.isExtension()) {
			// loader extends existing cube measure logic
			addMeasure(measureKey, columnName);
		}
		int measureIndex = cube.getMeasureIndex(cube.getDataPool().getMeasure(measureKey));
		loader.setMeasureIndex(measureIndex);
		measureLoaderMap.put(loader, columnName);
	}
	
	/**
	 * Add a ICellLoader for customer cell updates.
	 * @param measureKey
	 * @param columnName
	 * @param loader
	 */
	public void addCellLoader(String columnName, ICellLoader loader) {
		cellLoaderMap.put(columnName, loader);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getElement(de.xwic.cube.IDimension, de.xwic.etlgine.IRecord)
	 */
	@Override
	public IDimensionElement getElement(IDimension dim, IRecord record) throws ETLException {
		DimensionMapping dm = getDimensionMapping(dim);
		if (dm == null) {
			if (!enforceDimensionMapping) {
				return null;
			}
			throw new ETLException("No mapping for dimension " + dim.getKey());
		}
		return dm.mapElement(processContext, cube, record);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getMeasures()
	 */
	@Override
	public List<IMeasure> getMeasures() {
		return measures;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getValue(de.xwic.cube.IMeasure, de.xwic.etlgine.IRecord)
	 * /
	public Double getValue(IMeasure measure, IRecord record) throws ETLException {
		MeasureMapping mm = measureMap.get(measure);
		return mm.getValue(cube, record);
	}
	*/

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#getMeasureMapping(de.xwic.cube.IMeasure)
	 */
	@Override
	public MeasureMapping getMeasureMapping(IMeasure measure) {
		MeasureMapping mm = measureMap.get(measure);
		return mm;
	}
	
	@Override
	public DimensionMapping getDimensionMapping(IDimension dimension) {
		return dimMap.get(dimension);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#accept(de.xwic.etlgine.IRecord)
	 */
	@Override
	public boolean accept(IRecord record) throws ETLException {
		return true;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#clearCube(de.xwic.cube.ICube)
	 */
	@Override
	public void clearCube(ICube cube) {
		cube.clear();
		for (IMeasureLoader loader: measureLoaderMap.keySet()) {
			loader.clear();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#onAddCellValue(de.xwic.cube.Key, IMeasure, java.lang.Double, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void onAddCellValue(Key key, IMeasure measure, Double value, IRecord record) throws ETLException {
		// TODO check if using only one measure makes sense and improves performance
		for (Map.Entry<IMeasureLoader, String> entry : measureLoaderMap.entrySet()) {
			IMeasureLoader loader = entry.getKey();
			if (!loader.accept(cube, key, measure, value)) {
				continue;
			}
			String columnName = entry.getValue();
			Object count = record.getData(columnName);
			loader.setObjectFocus(count);
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.ICubeDataMapper#onCellProcessed(de.xwic.cube.Key, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void onCellProcessed(Key key, IRecord record) throws ETLException {
		ICell cell = null;
		for (Map.Entry<String, ICellLoader> entry : cellLoaderMap.entrySet()) {
			String columnName = entry.getKey();
			ICellLoader loader = entry.getValue();
			Object value = record.getData(columnName);
			if (value != null && cell == null) {
				// get cell
				cell = cube.getCell(key, true);
			}
			if (value != null) {
				loader.updateCell(cube, key, cell, columnName, value);
			}
		}
	}

	/**
	 * @return the enforceDimensionMapping
	 */
	public boolean isEnforceDimensionMapping() {
		return enforceDimensionMapping;
	}

	/**
	 * @param enforceDimensionMapping the enforceDimensionMapping to set
	 */
	public void setEnforceDimensionMapping(boolean enforceDimensionMapping) {
		this.enforceDimensionMapping = enforceDimensionMapping;
	}

	/**
	 * @return the skipMissingColumns
	 */
	public boolean isSkipMissingColumns() {
		return skipMissingColumns;
	}

	/**
	 * @param skipMissingColumns the skipMissingColumns to set
	 */
	public void setSkipMissingColumns(boolean skipMissingColumns) {
		this.skipMissingColumns = skipMissingColumns;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		sb.append(getClass().getSimpleName()).append("], [Cube:").append(cube.getKey()).append("]");
		return sb.toString();
	}
}
