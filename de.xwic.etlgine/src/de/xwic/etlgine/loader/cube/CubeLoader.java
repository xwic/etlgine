/*
 * de.xwic.etlgine.loader.cube.CubeLoader 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.ICube;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.Key;
import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IRecord;

/**
 * Loads the data into a cube.
 * @author lippisch
 */
public class CubeLoader extends AbstractLoader {

	private IDataPoolProvider dataPoolProvider;
	private DataPoolInitializer dataPoolInitializer = null;
	private IDataPool dataPool;
	private ICube cube;
	
	private String targetCubeKey = null;
	private ICubeDataMapper dataMapper = null;
	
	/**
	 * Constructor.
	 * @param dataPoolProvider
	 */
	public CubeLoader(IDataPoolProvider dataPoolProvider) {
		this.dataPoolProvider = dataPoolProvider;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void initialize(IContext context) throws ETLException {
		super.initialize(context);
		dataPool = dataPoolProvider.getDataPool();
		if (targetCubeKey == null) {
			throw new ETLException("The target cube key is not specified.");
		}
		if (dataMapper == null) {
			throw new ETLException("No ICubeDataMapper specified.");
		}
		if (dataPoolInitializer != null) {
			try {
				dataPoolInitializer.verify(dataPool);
			} catch (Exception e) {
				throw new ETLException("Error verifying DataPool integerity.", e);
			}
		}
		
		if (!dataPool.containsCube(targetCubeKey)) {
			throw new ETLException("The DataPool does not contain a cube with the key " + targetCubeKey + ".");
		}
		cube = dataPool.getCube(targetCubeKey);
		dataMapper.initialize(context, cube);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IContext context, IRecord record) throws ETLException {

		Key key = cube.createKey("");
		int idx = 0;
		for (IDimension dim : cube.getDimensions()) {
			
			IDimensionElement element = dataMapper.getElement(dim, record);
			if (element == null) {
				// invalid data
				return;
			}
			key.setDimensionElement(idx++, element);
			
		}
		
		for (IMeasure measure : dataMapper.getMeasures()) {
			Double value = dataMapper.getValue(measure, record);
			if (value != null) {
				cube.setCellValue(key, measure, value);
			}
		}
		
	}

	/**
	 * @return the targetCubeKey
	 */
	public String getTargetCubeKey() {
		return targetCubeKey;
	}

	/**
	 * @param targetCubeKey the targetCubeKey to set
	 */
	public void setTargetCubeKey(String targetCubeKey) {
		this.targetCubeKey = targetCubeKey;
	}

	/**
	 * @return the dataPoolInitializer
	 */
	public DataPoolInitializer getDataPoolInitializer() {
		return dataPoolInitializer;
	}

	/**
	 * @param dataPoolInitializer the dataPoolInitializer to set
	 */
	public void setDataPoolInitializer(DataPoolInitializer dataPoolInitializer) {
		this.dataPoolInitializer = dataPoolInitializer;
	}

	/**
	 * @return the dataMapper
	 */
	public ICubeDataMapper getDataMapper() {
		return dataMapper;
	}

	/**
	 * @param dataMapper the dataMapper to set
	 */
	public void setDataMapper(ICubeDataMapper dataMapper) {
		this.dataMapper = dataMapper;
	}
	
}
