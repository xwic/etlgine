/**
 * 
 */
package de.xwic.etlgine.loader.cube;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.cube.CubeHandler;
import de.xwic.etlgine.cube.mapping.DimMapper;
import de.xwic.etlgine.cube.mapping.DimMappingDef;
import de.xwic.etlgine.cube.mapping.DimMappingDefDAO;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;
import de.xwic.etlgine.cube.mapping.DimMappingElementDefDAO;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.util.Validate;

/**
 * Used to map one or more columns to a dimension element based on 
 * mapping rules. The path of the resulting dimension element is stored
 * in a target column for later loading.
 * 
 * @author lippisch
 */
public class DimensionMappingTransformer extends AbstractTransformer {

	protected String dataPoolName = null;
	protected String[] sourceColumnNames = null;
	protected String separator = "/";
	protected String targetColumnName = null;
	protected String mappingName = null;
	protected String nullValue = "";
	
	protected IDataPool dataPool = null;
	protected DimMappingDef mappingDef = null;
	protected IColumn[] sourceColumns = null;
	protected IColumn targetColumn = null;
	protected List<DimMappingElementDef> mappingElements = null;
	protected List<DimMapper> mappers = null;
	protected IDimensionElement parentElm;
	
	protected boolean forceRemap = false;
	protected boolean autoCreateTargetColumn = false;
	
	private DimensionMappingTransformer onFailTransformer = null;
	protected Map<String, DimMappingElementDef> cachedDimMappingElementDef = new HashMap<String, DimMappingElementDef>();
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#initialize(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		Validate.notNull(mappingName, "No mappingName given.");
		Validate.notNull(targetColumnName, "No targetColumn given.");
		Validate.notNull(dataPoolName, "No dataPoolName given.");
		Validate.notNull(sourceColumnNames, "No sourceColumns given.");
		
		CubeHandler cubeHandler = CubeHandler.getCubeHandler(processContext);
		String conName = cubeHandler.getConnectionName(dataPoolName);
		Validate.notNull(conName, "No connection name to the sync tables is specified for the dataPoolManager " + dataPoolName);
		
		// load the mapping
	
		try {
			Connection connection = null;
			try {
				connection = JDBCUtil.openConnection(processContext, conName);
				DimMappingDefDAO dmdDAO = new DimMappingDefDAO(connection);
				mappingDef = dmdDAO.findMapping(mappingName);
				Validate.notNull(mappingDef, "A mapping with the name '" + mappingName + "' does not exist.");
				
				DimMappingElementDefDAO dmeDAO = new DimMappingElementDefDAO(connection);
				mappingElements = dmeDAO.listMappings(mappingName);
				
				// create mappers.
				mappers = new ArrayList<DimMapper>();
				for (DimMappingElementDef dme : mappingElements) {
					mappers.add(new DimMapper(dme));
				}
				
			} finally {
				connection.close();
			}
		} catch (SQLException se) {
			throw new ETLException("Error loading mapping data: " + se, se);
		}
		
		
		dataPool = cubeHandler.openDataPool(dataPoolName);
		
		if (mappingDef.getOnUnmapped() == DimMappingDef.Action.CREATE) {
			IDimension dim = dataPool.getDimension(mappingDef.getDimensionKey());
			parentElm = dim.parsePath(mappingDef.getUnmappedPath());
		}
		
		if (getOnFailTransformer() != null) {
			getOnFailTransformer().initialize(processContext);
		}

	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#preSourceProcessing(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {
		super.preSourceProcessing(processContext);
		
		IDataSet dataSet = processContext.getDataSet();
		
		sourceColumns = new IColumn[sourceColumnNames.length];
		for (int i = 0; i < sourceColumnNames.length; i++) {
			String colName = sourceColumnNames[i];
			if (!dataSet.containsColumn(colName)) {
				throw new ETLException("The DataSet does not contain the mapping relevant column '" + colName + "'");
			}
			sourceColumns[i] = dataSet.getColumn(colName);
		}
		
		if (!dataSet.containsColumn(targetColumnName)) {
			if (autoCreateTargetColumn) {
				targetColumn = dataSet.addColumn(targetColumnName);
			} else {
				throw new ETLException("The DataSet does not contain the target column " + targetColumnName);
			}
		}
		targetColumn = dataSet.getColumn(targetColumnName);
		
		if (getOnFailTransformer() != null) {
			getOnFailTransformer().preSourceProcessing(processContext);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#processRecord(de.xwic.etlgine.IProcessContext, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		super.processRecord(processContext, record);
		
		if (forceRemap || record.getData(targetColumn) == null) {
		
			doMappingByColumns(processContext, record, sourceColumns);
		}
		
	}

	/**
	 * @param processContext
	 * @param record
	 * @param sourceColumns
	 * @throws ETLException 
	 */
	protected void doMappingByColumns(IProcessContext processContext, IRecord record, IColumn... sourceColumns) throws ETLException {
		// build the source key
		StringBuilder sbSource = new StringBuilder();
		for (IColumn sourceCol : sourceColumns) {
			if (sbSource.length() > 0) {
				sbSource.append("/");
			}
			String val = record.getDataAsString(sourceCol);
			if (val != null) {
				// escape '/', '[' and ']'
				val = val.replace('/', '&').replace('[', '(').replace(']', ')');
				sbSource.append(val);
			} else {
				sbSource.append(nullValue);
			}
		}
		
		String value = sbSource.toString();
		
		doMapping(processContext, record, value);
	}

	/**
	 * @param processContext
	 * @param record
	 * @param value
	 * @throws ETLException 
	 */
	protected void doMapping(IProcessContext processContext, IRecord record, String value) throws ETLException {
		
		// lookup MappingDef
		DimMappingElementDef elmDef = cachedDimMappingElementDef.get(value);
		if (elmDef == null) {
			for (DimMapper mapper : mappers) {
				if (mapper.match(value)) {
					elmDef = mapper.getDimMappingElementDef();
					cachedDimMappingElementDef.put(value, elmDef);
					break;
				}
			}
		}
		// do the mapping...
		if (elmDef != null) {
			if (elmDef.isSkipRecord()) {
				record.setSkip(true);
			} else {
				record.setData(targetColumn, elmDef.getElementPath());
			}
		} else {
			// no mapping found
			elmDef = new DimMappingElementDef();
			
			switch (mappingDef.getOnUnmapped()) {
			case ASSIGN:
				//processContext.getMonitor().logInfo("Assigning unmapped value '" + value + "' to " + mappingDef.getUnmappedPath());
				record.setData(targetColumn, mappingDef.getUnmappedPath());
				elmDef.setElementPath(mappingDef.getUnmappedPath());
				break;
			case CREATE: {
				String[] path = value.split("/");
				IDimensionElement elm = parentElm;
				for (String key : path) {
					if (elm.containsDimensionElement(key)) {
						elm = elm.getDimensionElement(key);
					} else {
						elm = elm.createDimensionElement(key);
					}
				}
				record.setData(targetColumn, elm.getPath());
				elmDef.setElementPath(elm.getPath());
			}
			break;
			case FAIL:
				elmDef = null;
				if (getOnFailTransformer() != null) {
					getOnFailTransformer().processRecord(processContext, record);
				} else {
					throw new ETLException("Unable to map value '" + value + "' - Aborting process!");
				}
				break;
			case SKIP:
				record.setSkip(true);
				elmDef.setSkipRecord(true);
				break;
			}
			
			// cache unknown mapping
			if (elmDef != null) {
				cachedDimMappingElementDef.put(value, elmDef);
			}
		}
		
	}

	/**
	 * @return the dataPoolName
	 */
	public String getDataPoolName() {
		return dataPoolName;
	}

	/**
	 * @param dataPoolName the dataPoolName to set
	 */
	public void setDataPoolName(String dataPoolName) {
		this.dataPoolName = dataPoolName;
	}

	/**
	 * @return the sourceColumns
	 */
	public String[] getSourceColumns() {
		return sourceColumnNames;
	}

	/**
	 * The column(s) that should be concatinated to search for the right dimension element.
	 * @param sourceColumns the sourceColumns to set
	 */
	public void setSourceColumns(String... sourceColumns) {
		this.sourceColumnNames = sourceColumns;
	}

	/**
	 * The separator is used to concat multiple source columns.
	 * @return the separator
	 */
	public String getSeparator() {
		return separator;
	}

	/**
	 * The separator is used to concat multiple source columns.
	 * @param separator the separator to set
	 */
	public void setSeparator(String separator) {
		this.separator = separator;
	}

	/**
	 * The column the result path should be written into.
	 * @return the targetColumn
	 */
	public String getTargetColumn() {
		return targetColumnName;
	}

	/**
	 * The column the result path should be written into.
	 * @param targetColumn the targetColumn to set
	 */
	public void setTargetColumn(String targetColumn) {
		this.targetColumnName = targetColumn;
	}

	/**
	 * @return the mappingName
	 */
	public String getMappingName() {
		return mappingName;
	}

	/**
	 * @param mappingName the mappingName to set
	 */
	public void setMappingName(String mappingName) {
		this.mappingName = mappingName;
	}

	/**
	 * @return the nullValue
	 */
	public String getNullValue() {
		return nullValue;
	}

	/**
	 * @param nullValue the nullValue to set
	 */
	public void setNullValue(String nullValue) {
		this.nullValue = nullValue;
	}

	/**
	 * @return the forceRemap
	 */
	public boolean isForceRemap() {
		return forceRemap;
	}

	/**
	 * @param forceRemap the forceRemap to set
	 */
	public void setForceRemap(boolean forceRemap) {
		this.forceRemap = forceRemap;
	}

	/**
	 * @param sourceColumns the sourceColumns to set
	 */
	protected void setSourceColumns(IColumn[] sourceColumns) {
		this.sourceColumns = sourceColumns;
	}

	/**
	 * @return the autocreateTargetColumn
	 */
	public boolean isAutoCreateTargetColumn() {
		return autoCreateTargetColumn;
	}

	/**
	 * @param autocreateTargetColumn the autocreateTargetColumn to set
	 */
	public void setAutoCreateTargetColumn(boolean autocreateTargetColumn) {
		this.autoCreateTargetColumn = autocreateTargetColumn;
	}

	/**
	 * @param onFailTransformer the onFailTransformer to set
	 */
	public void setOnFailTransformer(DimensionMappingTransformer onFailTransformer) {
		this.onFailTransformer = onFailTransformer;
	}

	/**
	 * @return the onFailTransformer
	 */
	public DimensionMappingTransformer getOnFailTransformer() {
		return onFailTransformer;
	}
	
}
