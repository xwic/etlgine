package de.xwic.etlgine.loader.database;

import java.util.List;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Implementations of this class provide database-dependent queries for identity-related operations.
 * 
 * @author mbogdan
 * @see de.xwic.etlgine.loader.database.sqlserver.SqlServerIdentityManager
 */
public interface IdentityManager {

	/**
	 * <p>
	 * Based on the composite identity specified in pkColumns, returns true if the record exists in the target table, false otherwise.
	 * 
	 * @param jdbcTemplate
	 * @param processContext
	 * @param record
	 * @param pkColumns
	 * @param tablename
	 * @return
	 * @throws ETLException
	 */
	boolean recordExistsInTargetTable(final NamedParameterJdbcTemplate jdbcTemplate, final IProcessContext processContext,
			final IRecord record, final List<String> pkColumns, final String tablename) throws ETLException;

}
