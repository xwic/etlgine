package de.xwic.etlgine.loader.database.operation;

import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class InsertDatabaseOperation extends AbstractDatabaseOperation implements IDatabaseOperation {

	/**
	 * The Spring component used to insert DB entries.
	 */
	private SimpleJdbcInsert jdbcInsert;

	public InsertDatabaseOperation(final DataSource dataSource, final String tablename, final Integer batchSize) {
		this.jdbcInsert = new SimpleJdbcInsert(dataSource).withTableName(tablename);
		this.batchSize = batchSize;

		if (batchModeActive()) {
			batchParameters = new ArrayList<Map<String, Object>>();
		}
	}

	/**
	 * If batch mode is active, it adds the parameters of this row to the list. If the batchSize has been reached, it also executes the
	 * batch insert.
	 *
	 * If batch mode is inactive, it executes the insert.
	 *
	 * @param parameters
	 *            all the parameters needed to insert a single row, with column name as key and value to set as value
	 */
	protected void executeDatabaseOperation(final Map<String, Object> parameters) {
		if (batchModeActive()) {
			// Running in batch mode - execute only when batch limit has been reached
			batchParameters.add(parameters);

			if (batchParameters.size() >= batchSize) {
				// The batch limit has been reached, so execute the insert
				@SuppressWarnings("unchecked")
				Map<String, Object>[] insertBatchParametersArray = (HashMap<String, Object>[]) new HashMap[batchParameters.size()];

				// Send the insert statement along with all the computed parameters of this batch to the database
				jdbcInsert.executeBatch(batchParameters.toArray(insertBatchParametersArray));

				// Clear the parameters of the batch that has been currently executed
				batchParameters.clear();
			}
		} else {
			// Running in non-batch mode - execute insert after each record processing
			jdbcInsert.execute(parameters);
		}
	}

}
