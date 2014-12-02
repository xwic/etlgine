package de.xwic.etlgine.loader.database.operation;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractDatabaseOperation implements IDatabaseOperation {

	/**
	 * Specifies the maximum batch size for this database operations. If 'null', batch processing is not used.
	 */
	protected Integer batchSize;

	/**
	 * A list of SQL parameters (key=column name, value=value) to be set within the batch operation. When the size of this list reaches the
	 * batchSize, the batch insert is executed.
	 */
	protected List<Map<String, Object>> batchParameters;
	
	/**
	 * List of columns that needs to be excluded in the statement
	 */
	protected List<String> excludedColumns; 

	/**
	 * Flag used to indicate if the column names are escaped in the insert/update statements
	 */
	protected boolean escapeColumns = false;
	
	/**
	 * Performs the DB operation (insert or update) for the record.
	 *
	 * It uses the table metadata returned by the DB engine. This only works if the processContext.dataSet.columns uses the EXACT same
	 * column names as the target table.
	 *
	 * @param processContext
	 *            the processContext holding the target dataSet
	 * @param record
	 *            the record being inserted or updated
	 * @throws ETLException
	 */
	@Override
	public void execute(final IProcessContext processContext, final IRecord record) throws ETLException {
		// Map containing the target column names as key, and the value to be inserted as value
		Map<String, Object> parameters = prepareParameters(processContext, record);

		executeDatabaseOperation(parameters);
	}

	/**
	 * Based on the target columns found on the processContext.dataSet, it returns a list of SQL parameters (key=column name, value=value)
	 * to be set within the batch operation.
	 *
	 * The key of this map should contain EXACTLY the same names as the target column names!
	 *
	 * @param processContext
	 *            the processContext holding the target dataSet
	 * @param record
	 *            the record being inserted or updated
	 * @return all the parameters needed to insert or update a single row, with column name as key and value to set as value
	 * @throws ETLException
	 */
	protected Map<String, Object> prepareParameters(final IProcessContext processContext, final IRecord record) throws ETLException {
		List<IColumn> columns = processContext.getDataSet().getColumns();

		// Map containing the target column names as key, and the value to be inserted as value
		Map<String, Object> parameters = new HashMap<String, Object>(columns.size());

		// Prepare the parameters for insert / update
		for (IColumn column : columns) {
			if (null !=excludedColumns && excludedColumns.contains(column.getName())){
				continue;
			}
			// The key of this map should contain EXACTLY the same names as the target column names
			parameters.put(column.getName(), record.getData(column));
		}

		return parameters;
	}

	protected boolean batchModeActive() {
		return batchSize != null;
	}

	protected abstract void executeDatabaseOperation(Map<String, Object> parameters);

	
	/**
	 * @return the excludedColumns
	 */
	protected List<String> getExcludedColumns() {
		return excludedColumns;
	}

	
	/**
	 * @param excludedColumns the excludedColumns to set
	 */
	protected void setExcludedColumns(List<String> excludedColumns) {
		this.excludedColumns = excludedColumns;
	}

	
	/**
	 * @return the escapeColumns
	 */
	protected boolean isEscapeColumns() {
		return escapeColumns;
	}

	
	/**
	 * @param escapeColumns the escapeColumns to set
	 */
	protected void setEscapeColumns(boolean escapeColumns) {
		this.escapeColumns = escapeColumns;
	}

}
