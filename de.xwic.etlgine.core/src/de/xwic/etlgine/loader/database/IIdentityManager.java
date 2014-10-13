package de.xwic.etlgine.loader.database;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;

/**
 * Implementations of this class provide database-dependent queries for identity-related operations.
 *
 * @author mbogdan
 * @see de.xwic.etlgine.loader.database.sqlserver.SqlServerIdentityManager
 */
public interface IIdentityManager {

	/**
	 * <p/>
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
