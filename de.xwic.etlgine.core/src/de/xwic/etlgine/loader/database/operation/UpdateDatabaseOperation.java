package de.xwic.etlgine.loader.database.operation;

import de.xwic.etlgine.loader.database.springframework.simplejdbcupdate.SimpleJdbcUpdate;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateDatabaseOperation extends AbstractDatabaseOperation implements IDatabaseOperation {

	/**
	 * The Spring component used to insert DB entries.
	 */
	private SimpleJdbcUpdate jdbcUpdate;

	/**
	 * A list of column names forming the WHERE constraint for this update. They are usually taken from the PK column list.
	 */
	private List<String> whereColumnNames;

	public UpdateDatabaseOperation(final DataSource dataSource, final String tablename, final List<String> whereColumnNames,
			final Integer batchSize) {
		jdbcUpdate = new SimpleJdbcUpdate(dataSource).withTableName(tablename);
		jdbcUpdate.setRestrictingColumns(whereColumnNames);
		this.whereColumnNames = whereColumnNames;
		this.batchSize = batchSize;
	}
	
	public UpdateDatabaseOperation(final DataSource dataSource, final String tablename, final List<String> whereColumnNames,
			final Integer batchSize, final List<String> excludedColumns) {
		jdbcUpdate = new SimpleJdbcUpdate(dataSource).withTableName(tablename);
		jdbcUpdate.setRestrictingColumns(whereColumnNames);
		this.whereColumnNames = whereColumnNames;
		this.batchSize = batchSize;
		this.excludedColumns = excludedColumns;
	}

	/**
	 * If batch mode is active, it adds the parameters of this row to the list. If the batchSize has been reached, it also executes the
	 * batch update.
	 *
	 * If batch mode is inactive, it executes the update.
	 *
	 * TODO Bogdan - reactivate the batch update after finding a solution to the SimpleJdbcUpdate
	 *
	 * @param parameters
	 *            all the parameters needed to update a single row, with column name as key and value to set as value
	 */
	protected void executeDatabaseOperation(final Map<String, Object> parameters) {
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
		//				Map<String, Object>[] inserBatchParametersArray = (HashMap<String, Object>[]) new HashMap[batchParameters.size()];
		//
		//				// Send the insert statement along with all the computed parameters of this batch to the database
		//				jdbcUpdate.executeBatch(batchParameters.toArray(inserBatchParametersArray));
		//
		//				// Clear the parameters of the batch that has been currently executed
		//				batchParameters.clear();
		//			}
		//		} else {
		//			// Running in non-batch mode - execute insert after each record processing
		jdbcUpdate.execute(parameters, whereParameters);
		//		}
	}

	/**
	 * Extracts a map with the parameters of the WHERE clause.
	 *
	 * @param allParameters
	 *            the entire parameter list
	 * @return parameters of the WHERE clause.
	 */
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

}
