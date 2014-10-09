package de.xwic.etlgine.loader.database;

import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.loader.database.operation.IDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.InsertDatabaseOperation;
import de.xwic.etlgine.loader.database.operation.UpdateDatabaseOperation;

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

	// TODO
	private NamedParameterJdbcTemplate jdbcTemplate;

	// Connection properties
	// TODO - When is the connection committed?
	private String driverClassName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl;
	private String username;
	private String password;
	// private String catalogname;//TODO
	//RPF: Trying to implement schema definitions in JDBC Loader
	//	private String schemaName = null;//TODO

	/** The database-dependent identity manager */
	private IIdentityManager identityManager;

	private IDatabaseOperation insert;
	private IDatabaseOperation update;

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
	 * Initializes all the components.
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);

		// Validate dataSource parameters
		// TODO

		// Validate parameters based on mode
		DatabaseLoaderValidators.validateParameters(mode, pkColumns, identityManager, tablename);

		// Initialize the dataSource
		initDataSource();

		// Initialize the jdbcTemplate
		this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

		// Initialize the insert
		this.insert = new InsertDatabaseOperation(dataSource, tablename);

		// Initialize the update mode
		this.update = new UpdateDatabaseOperation(dataSource, tablename, pkColumns);
	}

	@Override
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {
	}

	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		try {
			switch (mode) {
			case INSERT:
				// doInsert(processContext, record, columns.values(), psInsert, pkColumn != null ? pkColumn : "Id");
				break;

			case UPDATE:
				// doUpdate(processContext, record);
				break;

			case INSERT_OR_UPDATE:
				// TODO Bogdan - this approach performs a select for each record, in order to determine if
				// it should perform an update or an insert

				// if performance is severely impacted by this approach, consider using SQL's MERGE command,
				// but with caution - http://www.mssqltips.com/sqlservertip/3074/use-caution-with-sql-servers-merge-statement/

				// another solution would be to check all against the database, create a Map<Boolean, IRecord> specifying if it exists
				// or not, and later determine based on this map if it's an insert or an update

				if (identityManager.recordExistsInTargetTable(jdbcTemplate, processContext, record, pkColumns, tablename)) {
					System.out.println("-------------------- performing update");

					update.execute(processContext, record);
				} else {
					System.out.println("-------------------- performing insert");

					insert.execute(processContext, record);
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
			throw new ETLException(msg, t);
			//			}
		}
	}

	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
	}

	private void initDataSource() {
		BasicDataSource basicDataSource = new BasicDataSource();
		basicDataSource.setDriverClassName(getDriverClassName());
		basicDataSource.setUrl(getConnectionUrl());
		basicDataSource.setUsername(getUsername());
		basicDataSource.setPassword(getPassword());
		// basicDataSource.setMaxActive(100);
		// basicDataSource.setMaxIdle(30);
		// basicDataSource.setMaxWait(10000);

		monitor.logInfo("Built a new dataSource for the DatabaseLoader: [driverClassName=" + basicDataSource.getDriverClassName()
				+ ", url=" + basicDataSource.getUrl() + ", username=" + basicDataSource.getUsername() + "]");

		this.dataSource = basicDataSource;
	}

	public Mode getMode() {
		return mode;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public List<String> getPkColumns() {
		return pkColumns;
	}

	public void setPkColumns(List<String> pkColumns) {
		this.pkColumns = pkColumns;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public IIdentityManager getIdentityManager() {
		return identityManager;
	}

	public void setIdentityManager(IIdentityManager identityManager) {
		this.identityManager = identityManager;
	}

}
