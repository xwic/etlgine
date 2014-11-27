package de.xwic.etlgine.loader.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.loader.database.operation.IDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.InsertDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.UpdateDatabaseOperation;
import de.xwic.etlgine.util.RecordUtil;

/**
 * Loader that inserts or updates rows in a database table.
 *
 * It is an alternative to the {@link de.xwic.etlgine.loader.jdbc.JDBCLoader}, with some enhancements and some stripped capabilities, as
 * follows:
 *
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
 * @author Ionut
 */
public class DatabaseLoader extends AbstractLoader {

	private static final Log log = LogFactory.getLog(DatabaseLoader.class);

	/**
	 * The modes in which this loader could operate.
	 *
	 * @author mbogdan
	 */
	public enum Mode {
		/**
		 * In this mode, the loader determines if the processed record exists or not, based on the specified primary key column(s). If it
		 * exists performs an UPDATE, otherwise it INSERT a new record.
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

	/** The dataSource that provides connections. */
	private DataSource dataSource;

	/** If true, it commits and closes the connection onProcessFinished. Set it to false to be able to commit in the finalizers. */
	private boolean commitOnProcessFinished;

	/** A template used to execute DB queries with named parameters. */
	private NamedParameterJdbcTemplate jdbcTemplate;

	/** The name of the connection, used to take the connection configuration form the server.properties file. */
	private String connectionId;

	/** The database-dependent identity manager */
	private IIdentityManager identityManager;

	/** The component that handles database insertions. */
	private IDatabaseOperation insert;

	/** The component that handles database updates. */
	private IDatabaseOperation update;

	/** The target table name. */
	private String tablename;

	/** Defines how many insert or update operations should be included in one batch. If 'null', batch mode is deactivated. */
	private Integer batchSize;

	/**
	 * Holds the list of columns that are forming the composite PK on target side.
	 *
	 * It's primarily used to determine whether this operation is an INSERT or an UPDATE when running in the INSERT_OR_UPDATE mode.
	 *
	 * The order of the columns in this list has to be the same as in the definition of the target database.
	 */
	private List<String> pkColumns;

	/**
	 * List of columns to exclude from the insert or update
	 */
	private List<String> excludedColumns;

	/**
	 * Character used to quote table or column names
	 */
	private String quoteChar = null;

	/**
	 * Flag used to truncate the table. This flag must be used with the insert mode.
	 */
	private boolean truncateTable = false;

	/**
	 * Indicates if the table content purged either by truncate or by delete
	 */
	private boolean tablePurged = false;

	@Override
	public void initialize(final IProcessContext processContext) throws ETLException {
		super.initialize(processContext);

		// Validate parameters based on mode
		DatabaseLoaderValidators.validateParameters(connectionId, mode, pkColumns, identityManager, tablename);

		// Initialize the dataSource which will provide connections inside the Spring framework
		this.dataSource = DataSourceFactory.buildDataSource(connectionId, processContext);

		// Share the connection (to be reused within the finalizers)
		Connection connection = DataSourceUtils.getConnection(dataSource);
		JDBCUtil.setSharedConnection(processContext, connectionId, connection);

		if (quoteChar == null) {
			quoteChar = JDBCUtil.getIdentifierSeparator(connection);
		}
		tablePurged = false;

		// Initialize the jdbcTemplate
		this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

		// Initialize the insert
		this.insert = new InsertDatabaseOperation(dataSource, tablename, batchSize, excludedColumns);

		// Initialize the update mode
		this.update = new UpdateDatabaseOperation(dataSource, tablename, pkColumns, batchSize, excludedColumns);
	}

	@Override
	public void preSourceProcessing(final IProcessContext processContext) throws ETLException {
		super.preSourceProcessing(processContext);
		if (truncateTable && !tablePurged) {
			// truncate table only once for source processing, set to false in method initialize
			truncateTable(DataSourceUtils.getConnection(dataSource));
		}
	}

	@Override
	public void processRecord(final IProcessContext processContext, final IRecord record) throws ETLException {
		try {
			switch (mode) {
			case INSERT:
				insert(processContext, record);
				break;

			case UPDATE:
				update(processContext, record);
				break;

			case INSERT_OR_UPDATE:
				// TODO Bogdan - this approach performs a select for each record, in order to determine if it should perform an update 
				// or an insert

				// to speed thing up, select all existing records into a Set<existing PKs> and later determine based on this set
				// if it's an insert or an update
				if (identityManager.recordExistsInTargetTable(jdbcTemplate, processContext, record, pkColumns, tablename)) {
					update(processContext, record);
				} else {
					insert(processContext, record);
				}
				break;

			default:
				throw new ETLException("Invalid DatabaseLoader.mode specified. Available modes: 'INSERT', 'UPDATE', 'INSERT_OR_UPDATE'");
			}
		} catch (Throwable t) {
			record.markInvalid(t.getLocalizedMessage());
			if (commitOnProcessFinished) {
				ConnectionUtils.rollbackConnection(DataSourceUtils.getConnection(dataSource));
			}
			String msg = "Cannot process record " + processContext.getRecordsCount();
			throw new ETLException(msg, t);
		}
	}

	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
		if (commitOnProcessFinished) {
			ConnectionUtils.commitConnection(DataSourceUtils.getConnection(dataSource));
		}
	}

	/**
	 * Inserts the record into the target database.
	 *
	 * @param processContext
	 *            the processContext holding the target dataSet
	 * @param record
	 *            the record being inserted
	 * @throws ETLException
	 */
	private void insert(final IProcessContext processContext, final IRecord record) throws ETLException {
		monitor.logDebug("Inserting record with PK: " + RecordUtil.buildPKString(record, pkColumns) + " into target table: " + tablename);

		insert.execute(processContext, record);
	}

	/**
	 * Updates the record into the target database, based on the pkColumns.
	 *
	 * @param processContext
	 *            the processContext holding the target dataSet
	 * @param record
	 *            the record being updated
	 * @throws ETLException
	 */
	private void update(final IProcessContext processContext, final IRecord record) throws ETLException {
		monitor.logDebug("Updating record with PK: " + RecordUtil.buildPKString(record, pkColumns) + " into target table: " + tablename);

		update.execute(processContext, record);
	}

	/**
	 * Truncate table
	 * 
	 * @throws ETLException
	 */
	protected void truncateTable(Connection conn) throws ETLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int rows;
			try {
				// try TRUNCATE TABLE
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + getTablenameQuoted());
				rs.next();
				rows = rs.getInt(1);
				stmt.executeUpdate("TRUNCATE TABLE " + getTablenameQuoted());
			} catch (SQLException e) {
				// try DELETE FROM
				rows = stmt.executeUpdate("DELETE FROM " + getTablenameQuoted());
			}
			tablePurged = true;
			processContext.getMonitor().logInfo("TRUNCATED TABLE " + getTablenameQuoted() + " - " + rows + " rows have been deleted.");
		} catch (SQLException e) {
			throw new ETLException("Error truncating table: " + e, e);
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.warn("Ignore an exception that was thrown when closing the truncate/delete statement", e);
				}
			}
		}

	}

	/**
	 * Returns the JDBC identifier separator. On SQLException " is used as fallback.
	 * 
	 * @param connection
	 * @return
	 */
	public static String getIdentifierSeparator(Connection connection) {
		String identifierSeparator = null;
		try {
			identifierSeparator = connection.getMetaData().getIdentifierQuoteString();
		} catch (SQLException e) {
			identifierSeparator = "\""; // use this
			log.warn("Error reading identifierQuoteString", e);
		}
		return identifierSeparator;
	}

	protected String getTablenameQuoted() {
		StringBuilder sql = new StringBuilder();
		sql.append(quoteChar).append(tablename).append(quoteChar);
		return sql.toString();
	}

	// Getters and setters
	public Mode getMode() {
		return mode;
	}

	public void setMode(final Mode mode) {
		this.mode = mode;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(final String tablename) {
		this.tablename = tablename;
	}

	public void setPkColumns(final List<String> pkColumns) {
		this.pkColumns = pkColumns;
	}

	public void setIdentityManager(final IIdentityManager identityManager) {
		this.identityManager = identityManager;
	}

	public void setBatchSize(final Integer batchSize) {
		this.batchSize = batchSize;
	}

	public void setConnectionId(final String connectionId) {
		this.connectionId = connectionId;
	}

	public boolean isCommitOnProcessFinished() {
		return commitOnProcessFinished;
	}

	public void setCommitOnProcessFinished(boolean commitOnProcessFinished) {
		this.commitOnProcessFinished = commitOnProcessFinished;
	}

	/**
	 * @param excludedColumns
	 *            the excludedColumns to set
	 */
	public void setExcludedColumns(List<String> excludedColumns) {
		this.excludedColumns = excludedColumns;
	}

	/**
	 * @return the truncateTable
	 */
	public boolean isTruncateTable() {
		return truncateTable;
	}

	/**
	 * @param truncateTable
	 *            the truncateTable to set
	 */
	public void setTruncateTable(boolean truncateTable) {
		this.truncateTable = truncateTable;
	}

}
