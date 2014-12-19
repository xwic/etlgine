/*
 * Copyright (c) 2010 NetApp, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.transformer;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Properties;

import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;

/**
 * @author ronnyp
 *
 */
public class ColumnMappingTransformer extends AbstractTransformer {

	private String colPropertyFileName = "ColumnMapping.properties";
	
	/**
	 * Creates the mapping with given file name. E. g. ColumnMapping.properties
	 * located in the config folder. 
	 * 
	 * @param propertyFileName
	 */
	public ColumnMappingTransformer(String propertyFileName) {
		if (propertyFileName == null) {
			throw new IllegalArgumentException("Given properties file name is NULL!");
		}
		
		this.colPropertyFileName = propertyFileName;
	}

	/**
	 * Creates the mapping of ColumnMapping.properties located in the config folder. 
	 */
	public ColumnMappingTransformer() {
	}
	
	
	
	
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#preSourceProcessing(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void preSourceProcessing(IProcessContext processContext)
			throws ETLException {
		super.preSourceProcessing(processContext);
		
		Properties columnMapping = new Properties();
		try {
			String rootPath = processContext.getProperty("rootPath");
			columnMapping.load(new FileInputStream(rootPath + "/config/" + colPropertyFileName));
		} catch (Exception e) {
			processContext.getMonitor().logError("Cannot load Properties for Column Mapping!", e);
			//cannot continue here, this must get fixed!!!
			throw new ETLException(e);
		}
		
		for (Iterator<Object> iterator = columnMapping.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			String value = columnMapping.getProperty(key);
			processContext.getMonitor().logInfo("KEY: [" + key + "]   VALUE: [" + value + "]"); 

			if (value != null && value.length() > 0) {
				if (!processContext.getDataSet().containsColumn(key)) {
					processContext.getMonitor().logWarn("There is a column defined, but not found in dataset of ETL: " + key);
					continue;
				}
				
				IColumn col = processContext.getDataSet().getColumn(key);
				col.setTargetName(value);
			} else {
				processContext.getMonitor().logWarn("There is a column defined, but not found a value for it in the properties file: " + key);
			}
		}
	}
}
