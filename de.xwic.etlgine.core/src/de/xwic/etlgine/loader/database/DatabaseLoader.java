package de.xwic.etlgine.loader.database;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.loader.database.operation.IDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.InsertDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.UpdateDatabaseOperation;
import de.xwic.etlgine.util.RecordUtil;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

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
 */
public class DatabaseLoader extends AbstractLoader {

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

	/**
	 * The mode in which the loader operates. Defaults to INSERT_OR_UPDATE, as it is the safest into getting data.
	 */
	private Mode mode = Mode.INSERT_OR_UPDATE;

	/**
	 * A template used to execute DB queries with named parameters.
	 */
	private NamedParameterJdbcTemplate jdbcTemplate;

	/**
	 * The name of the connection, used to take the connection configuration form the server.properties file.
	 */
	private String connectionName;

	/**
	 * The database-dependent identity manager
	 */
	private IIdentityManager identityManager;

	/**
	 * The component that handles database insertions.
	 */
	private IDatabaseOperation insert;

	/**
	 * The component that handles database updates.
	 */
	private IDatabaseOperation update;

	/**
	 * The target table name.
	 */
	private String tablename;

	/**
	 * Defines how many insert or update operations should be included in one batch. If 'null', batch mode is deactivated.
	 */
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
	 * Initializes all the components.
	 */
	@Override
	public void initialize(final IProcessContext processContext) throws ETLException {
		super.initialize(processContext);

		// Validate parameters based on mode
		DatabaseLoaderValidators.validateParameters(connectionName, mode, pkColumns, identityManager, tablename);

		// Initialize the dataSource which will provide connections inside the Spring framework
		// TODO Bogdan - When is the connection committed?
		DataSource dataSource = ConnectionUtil.initDataSource(connectionName, processContext);

		// Initialize the jdbcTemplate
		this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

		// Initialize the insert
		this.insert = new InsertDatabaseOperation(dataSource, tablename, batchSize);

		// Initialize the update mode
		this.update = new UpdateDatabaseOperation(dataSource, tablename, pkColumns, batchSize);
	}

	@Override
	public void preSourceProcessing(final IProcessContext processContext) throws ETLException {
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
				// TODO Bogdan - this approach performs a select for each record, in order to determine if
				// it should perform an update or an insert

				// if performance is severely impacted by this approach, consider using SQL's MERGE command,
				// but with caution - http://www.mssqltips.com/sqlservertip/3074/use-caution-with-sql-servers-merge-statement/

				// another solution would be to check all against the database, create a Map<Boolean, IRecord> specifying if it exists
				// or not, and later determine based on this map if it's an insert or an update
				if (identityManager.recordExistsInTargetTable(jdbcTemplate, processContext, record, pkColumns, tablename)) {
					update(processContext, record);
				} else {
					insert(processContext, record);
				}
				break;

			default:
				throw new ETLException(
						"Invalid JDBCLoader.mode specified. Available modes: 'INSERT', 'UPDATE', 'INSERT_OR_UPDATE (deprecated)', 'INSERT_OR_UPDATE_COMPOSITE_PK.'");
			}
		} catch (Throwable t) {
			record.markInvalid(t.getLocalizedMessage());
			String msg = "Cannot process record " + processContext.getRecordsCount();
			throw new ETLException(msg, t);
		}
	}

	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
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
		monitor.logInfo("Inserting record with PK: " + RecordUtil.buildPKString(record, pkColumns) + " into target table: " + tablename);

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
		monitor.logInfo("Updating record with PK: " + RecordUtil.buildPKString(record, pkColumns) + " into target table: " + tablename);

		update.execute(processContext, record);
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

	public void setConnectionName(final String connectionName) {
		this.connectionName = connectionName;
	}

}
