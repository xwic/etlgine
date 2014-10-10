package de.xwic.etlgine.loader.database.operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.loader.database.springframework.simplejdbcupdate.SimpleJdbcUpdate;

public class UpdateDatabaseOperation implements IDatabaseOperation {

	// TODO
	private SimpleJdbcUpdate jdbcUpdate;

	private List<String> whereColumnNames;
	
	/** Specifies the maximum batch size for update operations. If 'null', batch processing is not used. */
	protected Integer batchSize;

	public UpdateDatabaseOperation(final DataSource dataSource, final String tablename, final List<String> whereColumnNames, final Integer batchSize) {
		jdbcUpdate = new SimpleJdbcUpdate(dataSource).withTableName(tablename);
		jdbcUpdate.setRestrictingColumns(whereColumnNames);
		this.whereColumnNames = whereColumnNames;
		this.batchSize = batchSize;
	}

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

	private void executeDatabaseOperation(final Map<String, Object> parameters) {
		// Additionally to the parameters that we need to set during the UPDATE command, that are prepared by the prepareParameters()
		// method, we also need the parameters for the WHERE clause. These parameters are already incorporated in the 'parameters' map,
		// so we just need an extract.
		Map<String, Object> whereParameters = extractWhereParameters(parameters);
		
		//		if (batchModeActive()) {
		//			// Running in batch mode - execute only when batch limit has been reached
		//			batchParameters.add(parameters);
		//
		//			if (batchParameters.size() >= batchSize) {
		//				// The batch limit has been reached, so execute the insert
		//				@SuppressWarnings("unchecked")
		//				HashMap<String, Object>[] inserBatchParametersArray = (HashMap<String, Object>[]) new HashMap[batchParameters.size()];
		//
		//				// Send the insert statement along with all the computed parameters of this batch to the database
		//				jdbcUpdate.executeBatch(batchParameters.toArray(inserBatchParametersArray));
		//
		//				// Clear the parameters of the batch that has been currently executed
		//				batchParameters.clear();
		//			}
		//		} else {
		// Running in non-batch mode - execute insert after each record processing
		jdbcUpdate.execute(parameters, whereParameters);
		//		}
	}

	private Map<String, Object> extractWhereParameters(final Map<String, Object> allParameters) {
		Map<String, Object> whereParameters = new HashMap<String, Object>(whereColumnNames.size());

		for (Map.Entry<String, Object> parameter : allParameters.entrySet()) {
			if (whereColumnNames.contains(parameter.getKey())) {
				// This is a column used in the WHERE clause, most probably part of the PK
				whereParameters.put(parameter.getKey(), parameter.getValue());
			}
		}

		return whereParameters;
	}

	private boolean batchModeActive() {
		return batchSize != null;
	}

}
