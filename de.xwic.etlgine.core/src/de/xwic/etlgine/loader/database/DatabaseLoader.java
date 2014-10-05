package de.xwic.etlgine.loader.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Loader that inserts or updates rows in a database table.
 * 
 * It is an alternative to the {@link de.xwic.etlgine.loader.jdbc.JDBCLoader.JDBCLoader}, with some enhancements and some stripped
 * capabilities, as follows:
 * 
 * <pre>
 * Enhancements:
 * - supports composite PKs when updating rows,
 * - the INSERT_OR_UPDATE is working,
 * 
 * Missing features:
 * - all the auto-create features (tables, columns, etc.),
 * - all the features that are changing data structure (alter columns).
 * </pre>
 * 
 * @author mbogdan
 *
 */
public class DatabaseLoader extends AbstractLoader {

	/**
	 * The modes in which this loader could operate.
	 * 
	 * @author mbogdan
	 *
	 */
	public enum Mode {
		/**
		 * In this mode, the loader determines if the processed record exists or not, based on the specified primary key column(s). If it
		 * exists performs an UPDATE, otherwise it INSERTs a new record.
		 */
		INSERT_OR_UPDATE,

		/**
		 * In this mode, the loader inserts the processed record into the target table. Will fail with an exception if the record already
		 * exist, based on the primary key column(s).
		 */
		INSERT,

		/**
		 * In this mode, the loader updates the processed record onto the target table. If the record is not found, no database changes will
		 * be performed, and no exceptions will be thrown.
		 */
		UPDATE
	}

	/** The mode in which the loader operates. Defaults to INSERT_OR_UPDATE, as it is the safest into getting data. */
	private Mode mode = Mode.INSERT_OR_UPDATE;

	private DataSource dataSource;

	private NamedParameterJdbcTemplate jdbcTemplate;

	// Connection properties
	private String sharedConnectionName;
	private String connectionName;
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl;
	private String username;
	private String password;
	private String catalogname;
	//RPF: Trying to implement schema definitions in JDBC Loader
	//	private String schemaName = null;

	/** The target connection. */
	private Connection connection;

	/** The target table name. */
	private String tablename;

	/**
	 * Holds the list of columns that are forming the composite PK on target side.
	 * 
	 * It's primarily used to determine whether this operation is an INSERT or an UPDATE when running in the INSERT_OR_UPDATE mode.
	 * 
	 * The order of the columns in this list has to be the same as in the definition of the target database.
	 */
	private List<String> pkColumns;

	/**
	 * Initializes or reuses the shared connection to where the target table resides.
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);

		// Validate parameters
		DatabaseLoaderValidators.validateMode(mode, pkColumns);

		// Establish target connection
				connection = ConnectionUtil.getConnection(processContext, monitor, sharedConnectionName, connectionName, driverName, connectionUrl,
						username, password);

		// Initialize the dataSource
		// initDataSource();

		// Initialize the jdbcTemplate
		this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {
	}

	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		try {
			switch (mode) {
			case INSERT:
				//				doInsert(processContext, record, columns.values(), psInsert, pkColumn != null ? pkColumn : "Id");
				break;

			case UPDATE:
				//				doUpdate(processContext, record);
				break;

			//			case INSERT_OR_UPDATE:
			//				if (Validate.equals(newIdentifierValue, record.getData(newIdentifierColumn))) {
			//					doInsert(processContext, record, columns.values(), psInsert, pkColumn);
			//				} else {
			//					doUpdate(processContext, record);
			//				}
			//				break;

			case INSERT_OR_UPDATE:
				// TODO Bogdan - this approach performs a select for each record, in order to determine if
				// it should perform an update or an insert

				// if performance is severely impacted by this approach, consider using SQL's MERGE command,
				// but with caution - http://www.mssqltips.com/sqlservertip/3074/use-caution-with-sql-servers-merge-statement/

				// Determine if the record (source) exists in the target DB, based on the provided composite PK: pkColumns
				// - select ID from TARGET_TABLE where (pkColumns.get(0) = record.getData(pkColumns.get(0)), pkColumns.get(1) =
				// record.getData(pkColumns.get(1)), ...)
				// COLUMN_NAME = value for COLUMN_NAME
				if (recordExistsInTargetTable(processContext, record)) {
					System.out.println("-------------------- performing update");
					//					doUpdate(processContext, record);
				} else {
					System.out.println("-------------------- performing insert");
					//					doInsert(processContext, record, columns.values(), psInsert, pkColumn);
				}
				break;

			default:
				throw new ETLException(
						"Invalid JDBCLoader.mode specified. Available modes: 'INSERT', 'UPDATE', 'INSERT_OR_UPDATE (deprecated)', 'INSERT_OR_UPDATE_COMPOSITE_PK.'");
			}
		} catch (Throwable t) {
			record.markInvalid(t.getLocalizedMessage());
			String msg = "Cannot process record " + processContext.getRecordsCount();
			//			if (skipError) {
			//				processContext.getMonitor().logError(msg, t);
			//			} else {
			//				throw new ETLException(msg, t);
			//			}
		}
	}

	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
	}

//	private void initDataSource() {
//		BasicDataSource basicDataSource = new BasicDataSource();
//		basicDataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//		basicDataSource.setUrl("jdbc:sqlserver://localhost:1433;databaseName=option4");
//		basicDataSource.setUsername("option4");
//		basicDataSource.setPassword("option4");
//		// basicDataSource.setMaxActive(100);
//		// basicDataSource.setMaxIdle(30);
//		// basicDataSource.setMaxWait(10000);
//
//		this.dataSource = basicDataSource;
//	}

	private boolean recordExistsInTargetTable(final IProcessContext processContext, final IRecord record) throws ETLException {
		boolean recordExists = Boolean.FALSE;

		// check that the record is not null
		if (record == null) {
			throw new ETLException("Found null record while trying to find if the record exists in the table '" + tablename + "'.");
		}

		// build the query that checks if record exists based on all the pkColumns
		String existsQuery = null;//buildRecordExistsQuery(processContext, record);
		System.out.println("existsQuery: " + existsQuery);

		// execute the query and check the output
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(existsQuery);

			// if there are rows in the ResultSet, the isBeforeFirst() returns true
			if (rs.isBeforeFirst()) {
				recordExists = Boolean.TRUE;
			}

			// processContext.getMonitor().logInfo("TRUNCATED TABLE " + getTablenameQuoted() + " - " + rows +
			// " rows have been deleted.");
		} catch (SQLException e) {
			throw new ETLException("Error executing the exists query: " + existsQuery, e);
		}

		return recordExists;
	}

}
