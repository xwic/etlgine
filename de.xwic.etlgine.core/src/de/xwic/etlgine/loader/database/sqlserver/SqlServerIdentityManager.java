package de.xwic.etlgine.loader.database.sqlserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.loader.database.AbstractIdentityManager;
import de.xwic.etlgine.loader.database.DatabaseQuery;

/**
 * Provides queries adapted for the Microsoft SQL Server grammar for identity-related queries.
 * 
 * @author mbogdan
 *
 */
public class SqlServerIdentityManager extends AbstractIdentityManager {

	/**
	 * <p>
	 * Based on the composite identity specified in pkColumns, returns true if the record exists in the target table, false otherwise.
	 * 
	 * <p>
	 * For this it selects the first row of the target table matches the (composite) PK. All the columns of the pkColumns array are used to
	 * build the WHERE clause.
	 * 
	 * @param jdbcTemplate
	 * @param processContext
	 * @param record
	 * @param pkColumns
	 * @param tablename
	 * @return
	 * @throws ETLException
	 */
	public final boolean recordExistsInTargetTable(final NamedParameterJdbcTemplate jdbcTemplate, final IProcessContext processContext,
			final IRecord record, final List<String> pkColumns, final String tablename) throws ETLException {
		boolean recordExists = Boolean.FALSE;

		DatabaseQuery databaseQuery = buildRecordExistsQuery(processContext, record, pkColumns, tablename);
		String sql = databaseQuery.getQueryString().toString();
		Map<String, Object> parameters = databaseQuery.getParameters();

		// It returns something like this: [{name=Bob, id=1}, {name=Mary, id=2}]
		List<Map<String, Object>> resultList = jdbcTemplate.queryForList(sql, parameters);

		if (!resultList.isEmpty()) {
			recordExists = Boolean.TRUE;
		}

		return recordExists;
	}

	/**
	 * <p>
	 * Builds an SQL query that searches if a row already exists, based on all the columns involved in the PK.
	 * 
	 * <p>
	 * It uses named parameters for the PK values. The pattern for these parameters is: ":" + "COLUMN_NAME" + "_param"
	 * 
	 * Example:
	 * 
	 * <pre>
	 * select top 1 t.[CASEID], t.[ITEM_NO], t.[TRANSACTION_ID] from [dbo].[SAP_EXCEPTION_MANAGEMENT] t 
	 * where t.[CASEID] = :CASEID_param and t.[ITEM_NO] = :ITEM_NO_param and t.[TRANSACTION_ID] = :TRANSACTION_ID_param;
	 * </pre>
	 * 
	 * @param processContext
	 * @param record
	 * @return
	 * @throws ETLException
	 */
	private DatabaseQuery buildRecordExistsQuery(final IProcessContext processContext, final IRecord record, final List<String> pkColumns,
			final String tablename) throws ETLException {
		DatabaseQuery result = null;
		StringBuilder sqlQuery = null;
		Map<String, Object> parameters = null;

		List<IColumn> validatedPkColumns = new ArrayList<IColumn>();

		// transform the column names list provided in pkColumns into actual IColumns
		// if a column name is wrong, an exception will be thrown
		for (String columnName : pkColumns) {
			// Throws an exception if a column with this name does not exist
			IColumn column = processContext.getDataSet().getColumn(columnName);
			validatedPkColumns.add(column);
		}

		// Lazy-init the result variables
		result = new DatabaseQuery();
		sqlQuery = new StringBuilder();
		parameters = new HashMap<String, Object>(validatedPkColumns.size());

		// SELECT section
		sqlQuery.append("SELECT TOP 1 ");

		for (ListIterator<IColumn> it = validatedPkColumns.listIterator(); it.hasNext();) {
			IColumn column = it.next();

			sqlQuery.append("t.[").append(column.computeTargetName()).append("]");

			if (it.hasNext()) {
				sqlQuery.append(", ");
			}
		}

		// FROM section
		sqlQuery.append(" FROM ").append(tablename).append(" t");

		// WHERE section
		sqlQuery.append(" WHERE ");

		for (ListIterator<IColumn> it = validatedPkColumns.listIterator(); it.hasNext();) {
			IColumn column = it.next();

			String columnName = column.computeTargetName();
			String parameterName = columnName + "_param";

			sqlQuery.append("t.[").append(columnName).append("] = :").append(parameterName);
			parameters.put(parameterName, record.getData(column));

			if (it.hasNext()) {
				sqlQuery.append(" and ");
			}
		}

		result.setQueryString(sqlQuery);
		result.setParameters(parameters);
		return result;
	}
}
