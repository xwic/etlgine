package de.xwic.etlgine.loader.database.operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

public abstract class AbstractDatabaseOperation implements IDatabaseOperation {

	/** Specifies the maximum batch size for insert operations. If 'null', batch processing is not used. */
	protected Integer batchSize;

	/**
	 * A list of SQL parameters (key=column name, value=value) to be set within the batch operation. When the size of this list
	 * reaches the TODO, the batch insert is executed.
	 */
	protected List<Map<String, Object>> batchParameters;

	/**
	 * TODO - enhance docs...
	 * 
	 * This only works if the processContext.dataSet.columns uses the EXACT same column names as the target table.
	 * 
	 * @param processContext
	 * @param record
	 * @param pkColumns
	 * @throws ETLException
	 */
	@Override
	public void execute(final IProcessContext processContext, final IRecord record) throws ETLException {
		// Map containing the target column names as key, and the value to be inserted as value
		Map<String, Object> parameters = prepareParameters(processContext, record);

		executeDatabaseOperation(parameters);
	}

	private Map<String, Object> prepareParameters(final IProcessContext processContext, final IRecord record) throws ETLException {
		List<IColumn> columns = processContext.getDataSet().getColumns();

		// Map containing the target column names as key, and the value to be inserted as value
		Map<String, Object> parameters = new HashMap<String, Object>(columns.size());

		// Prepare the parameters for insert / update
		for (IColumn column : columns) {
			// The key of this map should contain EXACTLY the same names as the target column names
			parameters.put(column.getName(), record.getData(column));
		}

		return parameters;
	}

	protected abstract void executeDatabaseOperation(Map<String, Object> parameters);

}
