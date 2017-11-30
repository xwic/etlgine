/**
 * 
 */
package de.xwic.etlgine.loader.jdbc;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IDataSetColumnAdded;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.ITransformer;
import de.xwic.etlgine.extractor.jdbc.JDBCSource;
import de.xwic.etlgine.impl.Column;
import de.xwic.etlgine.impl.DataSet;
import de.xwic.etlgine.impl.ETLProcess;
import de.xwic.etlgine.impl.Record;
import de.xwic.etlgine.jdbc.DbColumnDef;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.util.Validate;

/**
 * @author lippisch
 *
 */
/**
 * @author JBORNEMA
 *
 */
public class JDBCLoader extends AbstractLoader {

	public enum Mode {
		INSERT,
		UPDATE,
		INSERT_OR_UPDATE
	}
	
	public enum OnColumnsType {
		/** Default mode, OnColumns uniquely identify the record */
		UNIQUE,
		/** OnColumns define measures and changing attributes, any other not excluded column uniquely identify the record */
		REVERSE_UNIQUE
	}
	
	public enum OnColumnsMode {
		/** Original replace on mechanism @deprecated use REPLACE_NEW instead (source can be incremental)*/
		REPLACE_ALL,
		
		/** Delete new records, that already exist.
		 *  Delete existing records that are going to be replaced.
		 *  (source can be incremental)
		 */
		REPLACE_NEW,
		
		/** OnColumns must uniquely identify a record!
		 *  Delete new records, that already exist.
		 *  Update old records, that have changed.
		 *  Delete new records, that have updated old records.
		 *  (source can be incremental, old records will remain)
		 */
		UPDATE,
		
		/** OnColumns must uniquely identify a record!
		 *  Delete new records, that already exist.
		 *  Update old records, that have changed or delete those that don't exist in source anymore.
		 *  Delete new records, that have updated old records.
		 *  (source MUST contain complete data set!)
		 */
		UPDATE_REMOVE_DELETED,

		/** History supporting extension of REPLACE_NEW mode.
		 *  Maintains history of records (incl. history start and end date)!
		 *  Delete new records, that already exist for current record (history end date = 9999-12-31).
		 *  Update old records' history end date, that have changed or got deleted (use history deleted date column
		 *  when source does not contain complete data set!).
		 *  When source is incremental and records get deleted the source requires a history deleted date column with
		 *  date(/time) of the deletion in database time-zone, or the source contains complete data set!
		 *  When history start or end columns exist in the source with values, these are used instead of current time!
		 */
		REPLACE_NEW_MAINTAIN_HISTORY
	}
	
	public enum AutoAlterColumnsMode {
		PRESERVE_KIND,
		ANY_TO_VARCHAR
	}
	
	/*
	 * When onColumns contains this tag, it's treated as a formula and not a column name
	 */
	public static String ONCOLUMNS_FORMULA = "/*{alias.}*/";
	protected static Map<String, String> SYNCHRONIZE_TABLE_ACCESS = Collections.synchronizedMap(new HashMap<String, String>());
	
	//protected static int[] INT_RANGE = {(int)Math.pow(-2, 31), (int)Math.pow(2, 31) - 1};
	//protected static long[] BIGINT_RANGE = {(long)Math.pow(-2, 63), (long)Math.pow(2, 63) - 1};
	
	private String sharedConnectionName = null;
	private String connectionName = null;
	// by default use the JTDS driver...
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl = null;
	private String username = null;
	private String password = null;
	private String catalogname = null;
	private String schemaname = null;
	private String tablename = null;
	private String originalTablename = null;
	private boolean enableObjectAlias = true;
	private boolean enableGlobalObjectAlias = true;
	private int objectAliasMaxLength = 30;

	private boolean autoCreateTable = false;
	private boolean autoCreateColumns = false;
	private boolean autoDetectColumnTypes = false;
	private boolean autoDetectColumnTypesNullable = true;
	private boolean autoDetectColumnTypesRunning = false;
	private boolean autoAlterColumns = false;
	private AutoAlterColumnsMode autoAlterColumnsMode = AutoAlterColumnsMode.PRESERVE_KIND;	
	private boolean autoDataTruncate = false;
	private boolean commitOnProcessFinished = true;
	
	private boolean ignoreMissingSourceColumns = false;
	private boolean ignoreMissingTargetColumns = false;
	private boolean treatEmptyAsNull = false;
	private boolean truncateTable = false;
	private boolean deleteTable = false;
	private boolean tablePurged = false;
	private boolean skipError = false;
	private boolean preventNotNullError = false;
	private boolean addColumnsToDataSet = true;
	private int batchSize = -1;
	private int batchCountInsert = 0;
	private int batchCountUpdate = 0;
	private int batchRecordsCountOffset = 0;
	
	private List<IRecord> batchInsertRecords = new ArrayList<IRecord>();
	private List<IRecord> batchUpdateRecords = new ArrayList<IRecord>();
	
	private boolean ignoreUnchangedRecords = false;
	
	private Mode mode = Mode.INSERT;
	private String pkColumn = null;
	private String newIdentifierColumn = null;
	private String newIdentifierValue = null;
	
	protected long insertCount = 0;
	protected long updateCount = 0;
	protected long deleteCount = 0;
	protected long processRecordCount = 0;
		
	private Connection connection = null;
	protected boolean reopenClosedConnection = true;
	private PreparedStatement psInsert = null;
	private PreparedStatement psUpdate = null;
	private Map<String, DbColumnDef> columns;
	private Set<String> ignoredColumns = new HashSet<String>();
	private Set<String> excludedColumns = new HashSet<String>();
	
	private boolean withTablock = false;
	private boolean simulatePkIdentity = true;
	private String pkSequence = null;
	
	private SqlDialect sqlDialect = SqlDialect.MSSQL;
	
	/** custom jdbc properties */
	private Properties properties = new Properties();
	
	private String is = null;

	private String autoIncrementColumn = JDBCUtil.PK_IDENTITY_COLUMN;
	private Object onColumnsMaxValue = null;
	private Object onColumnsLastMaxValue = null;
	
	// fields for "replace on" functionality that deleted old records based on the newly imported
	// fields for "update on" functionality that updates old records base on the newly imported (that get delete finally)
	private OnColumnsMode onColumnsMode = OnColumnsMode.REPLACE_ALL;

	private boolean onColumnsOnProcessFinished = false;
	private String[] onColumns = null;
	private boolean onColumnsIncludeMissingTargetColumns = true;
	private OnColumnsType onColumnsType = OnColumnsType.UNIQUE; 
	private String[] onColumnsExclude = null;
	private String[] onColumnsNullValue = null;
	private String[] onColumnsCollate = null;
	private String onColumnsHistoryStartDate = null;
	private String onColumnsHistoryEndDate = null;
	private String onColumnsHistoryDeletedDate = null;
	
	private Map<String, String> objectAliasByName = null;
	private Map<String, String> objectNameByAlias = null;
	
	private IDataSetColumnAdded columnAddedListener = null;
	private long nanoTimeProcessRecord = 0;
	private long nanoTimeExecuteBatch = 0;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void initialize(IProcessContext context) throws ETLException {
		
		// allow early initialization
		if (connection != null) {
			return;
		}
		
		super.initialize(context);
		
		if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(pkColumn, "PkColumn must be specified for UPDATE mode.");
		}
		if (mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(newIdentifierColumn, "NewIdentifierColumn must be specified for INSERT_OR_UPDATE mode.");
		}

		initConnection(context);

		tablePurged = false;

		/* 
		 * register JDBCLoader also as first Transformer to execute bulk updates before other Transformers
		 * to ensure all records are committed.
		 */
		
		IProcess iProcess = context.getProcess();
		if (iProcess instanceof ETLProcess) {
			ETLProcess etlProcess = (ETLProcess)iProcess;
			ITransformer transformer = new AbstractTransformer() {
				@Override
				public void postSourceProcessing(IProcessContext context) throws ETLException {
					if (connection != null) {
						// check open batch statements
						executeBatch();
						// don't call delete on columns as that might be to early (needs to wait for possible transformer adjustments)
					}
				}
			};
			etlProcess.addTransformer(transformer, 0);
		}
		
		/*
		 * Check replace on columns logic
		 */
		
		if (onColumnsNullValue != null && onColumns != null && onColumnsNullValue.length != 1 && onColumnsNullValue.length != onColumns.length) {
			
			context.getMonitor().logWarn("Replace on column configuration inconsistent on table " + getTablenameQuoted());
		}
	}
	
	/**
	 * 
	 * @param context
	 * @throws ETLException
	 */
	protected void initConnection(IProcessContext context) throws ETLException {
		if (connectionName == null) {
			if (connectionUrl == null) {
				throw new ETLException("No connection NAME or URL specified");
			}
			if (username == null) {
				throw new ETLException("No username specified");
			}
			if (password == null) {
				throw new ETLException("No password specified");
			}
			try {
				monitor.logInfo("Using direct connection - URL: " + connectionUrl);
				// initialize the driver
				try {
					Class.forName(driverName);
				} catch (ClassNotFoundException e) {
					throw new ETLException("The specified driver (" + driverName + ") can not be found.", e);
				}
				
				if (username != null) {
					properties.setProperty("user", username);
				}
				if (password != null) {
					properties.setProperty("password", password);
				}
				connection = DriverManager.getConnection(connectionUrl, properties);
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		} else {
			monitor.logInfo("Using named connection: " + connectionName);
			if (batchSize == -1) {
				batchSize = JDBCUtil.getBatchSize(context, connectionName);
			}
			String dialect = JDBCUtil.getDialect(context, connectionName);
			if (dialect != null && dialect.length() > 0) {
				// read dialect
				setSqlDialect(SqlDialect.valueOf(dialect));
			}
			try {
				if (sharedConnectionName != null) {
					connection = JDBCUtil.getSharedConnection(context, sharedConnectionName, connectionName);
				} else {
					connection = JDBCUtil.openConnection(context, connectionName);
				}
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		}
		
		if (is == null) {
			is = JDBCUtil.getIdentifierSeparator(connection);
		}
	}

	@Override
	public void postSourceProcessing(IProcessContext context) throws ETLException {
		super.postSourceProcessing(context);
		
		// called from finalizer as well
		postSourceProcessing();
	}

	/**
	 * Finish the open records, execute replace on columns.
	 * @throws ETLException 
	 */
	protected void postSourceProcessing() throws ETLException {
		if (connection != null) {
			// check open batch statements
			executeBatch();

			if (!onColumnsOnProcessFinished && onColumnsMode != null) {
				// if replace on columns is enabled and records existed, ensure consistency
				executeReplaceData();
			}			
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void onProcessFinished(IProcessContext context) throws ETLException {
		
		if (connection != null) {
			try {
				// check open batch statements
				executeBatch();
				if (psInsert != null) {
					psInsert.close();
					psInsert = null;
				}
				if (psUpdate != null) {
					psUpdate.close();
					psUpdate = null;
				}

				if (onColumnsOnProcessFinished && onColumnsMode != null) {
					// if replace on columns is enabled and records existed, ensure consistency
					executeReplaceData();
				}			

				if (sharedConnectionName == null) {
					// only close the connection if it is not shared!
					monitor.logInfo(this + " close connection");
					connection.close();
				} else {
					if (commitOnProcessFinished && !connection.getAutoCommit()) {
						// commit for shared connections
						monitor.logInfo(this + " commit transaction");
						connection.commit();
					}
				}
			} catch (SQLException e) {
				throw new ETLException("Error closing connection: " + e, e);
			}
			connection = null;
		}
		
		DecimalFormat df = new DecimalFormat("#,##0.00", DecimalFormatSymbols.getInstance(Locale.US));
		monitor.logInfo(this + " " + insertCount + " records inserted, " + updateCount + " records updated, " + deleteCount + " records deleted. " + 
						"Total nano time/record: " + df.format((double)(nanoTimeExecuteBatch + nanoTimeProcessRecord) / processRecordCount) + 
						", ProcessRecord: " + df.format((double)nanoTimeProcessRecord / processRecordCount) + 
						", ExecuteBatch: " + df.format((double)nanoTimeExecuteBatch / processRecordCount));
	}
	
	/**
	 * @throws ETLException 
	 * 
	 */
	protected void executeReplaceData() throws ETLException {
		if (onColumns == null) {
			// nothing to do
			return;
		}
		try {
			switch (onColumnsMode) {
			case REPLACE_ALL:
				// old @deprecated mode
				executeDeleteForReplace();
				break;
			case REPLACE_NEW:
			case REPLACE_NEW_MAINTAIN_HISTORY:
			case UPDATE:
			case UPDATE_REMOVE_DELETED:
				executeOnColumns();
				break;
			}
			
		} catch (SQLException e) {
			throw new ETLException(e);
		}
	}
	
	/**
	 * Deletes exiting records that will be replaced with the inserted once.
	 * @deprecated
	 * @throws SQLException
	 */
	protected void executeDeleteForReplace() throws SQLException {
		// exit if not applicable
		if (autoDetectColumnTypesRunning) {
			return;
		}
		
		String aCol = is + autoIncrementColumn + is;
		// check for replace
		if (onColumnsMaxValue != null) {
			onColumnsLastMaxValue = null;
			StringBuilder sql = new StringBuilder("delete t\n");
			sql
			.append("from " + getTablenameQuoted() + " t\n")
			.append("inner join (\n")
			.append("select distinct ");
			StringBuilder on = new StringBuilder();
			
			List<String> usedOnColumns = new ArrayList<String>();
			if (onColumnsType == OnColumnsType.UNIQUE && onColumns.length > 0) {
				// default mode
				usedOnColumns = Arrays.asList(onColumns);
			} else {
				// reverse mode
				for (DbColumnDef colDef : columns.values()) {
					
					IColumn iCol = colDef.getColumn();
					boolean skip = colDef.isReadOnly() || iCol == null || iCol.isExclude();
					
					if (!skip && onColumnsExclude != null) {
						for (String c : onColumnsExclude) {
							if (c.equalsIgnoreCase(colDef.getName())) {
								skip = true;
								break;
							}
						}
					}
					
					if (skip) {
						continue;
					}
					
					String addOnColumn = null;
					if (onColumns.length == 0) {
						// if onColumns is empty use all
						addOnColumn = colDef.getName();
					} else {
						int not_in_list = 0;
						for (String c : onColumns) {
							if (c.equalsIgnoreCase(colDef.getName())) {
								if (onColumnsType == OnColumnsType.UNIQUE) {
									// columns define a unique record
									addOnColumn = colDef.getName();
									break;
								}
							} else {
								// all columns except the onColumns once define a unique record
								not_in_list++;
							}
						}
						if (onColumnsType == OnColumnsType.REVERSE_UNIQUE && not_in_list == onColumns.length) {
							// all columns except the onColumns once define a unique record
							addOnColumn = colDef.getName();;
						}						
					}
					if (addOnColumn != null) {
						usedOnColumns.add(addOnColumn);						
					}
				}						
			}
			for (int i = 0; i < usedOnColumns.size(); i++) {
				String col = usedOnColumns.get(i);
				String nul = onColumnsNullValue != null ? 
						onColumnsNullValue.length > i ? onColumnsNullValue[i] : 
						onColumnsNullValue.length == 1 ? onColumnsNullValue[0] : null
						: null;
				String collate = onColumnsCollate != null && onColumnsCollate.length > i ? onColumnsCollate[i] : null;
				if (on.length() > 0) {
					sql.append(", ");
					on.append(" and ");
				}
				String col_n = "n." + is + col + is;
				String col_t = "t." + is + col + is;
				
				// check for formula
				if (Pattern.compile(ONCOLUMNS_FORMULA, Pattern.LITERAL + Pattern.CASE_INSENSITIVE).matcher(col).replaceAll(Matcher.quoteReplacement("")).length() < col.length()) {
					// column specifies a formula, set col_n and col_t accordingly
					col_n = Pattern.compile(ONCOLUMNS_FORMULA, Pattern.LITERAL + Pattern.CASE_INSENSITIVE).matcher(col).replaceAll(Matcher.quoteReplacement("n."));
					col_t = Pattern.compile(ONCOLUMNS_FORMULA, Pattern.LITERAL + Pattern.CASE_INSENSITIVE).matcher(col).replaceAll(Matcher.quoteReplacement("t."));
				}
				
				if (nul == null) {
					// default behavior
					if (collate == null) {
						sql.append(col_n + " as n" + i);
					} else {
						sql.append(col_n + " " + collate + " as n" + i);
					}
					on.append("n" + i + " = " + col_t);
				} else {
					// use coalesce on null values
					sql.append("coalesce(" + col_n + "," + nul + ")");
					if (collate != null) {
						sql.append(" " + collate);
					}
					sql.append(" as n" + i);
					on.append("n" + i + " = coalesce(" + col_t + "," + nul + ")");
				}
			}
			sql.append(" from " + getTablenameQuoted() + " n where " + aCol + " > ?\n")
			.append(") n on " + on + "\n")
			.append("where t." + aCol + " <= ?");

			PreparedStatement ps = connection.prepareStatement(sql.toString());
			ps.setObject(1, onColumnsMaxValue);
			ps.setObject(2, onColumnsMaxValue);
			try {
				monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted()); 
				int cnt = ps.executeUpdate();
				deleteCount += cnt;
				monitor.logInfo(this + " " + cnt + " records deleted.");
			} finally {
				onColumnsLastMaxValue = onColumnsMaxValue;
				// clear max id
				onColumnsMaxValue = null;
				ps.close();
			}
		}
	}

	/**
	 * Deletes exiting records that will be replaced with the inserted once.
	 * Supported modes are defined in OnColumnsMode.
	 * @throws SQLException
	 */
	protected void executeOnColumns() throws SQLException {
		// exit if not applicable
		if (autoDetectColumnTypesRunning) {
			return;
		}
		//connection.commit();
		final String[] psSql = new String[1];
		String t = is + "t" + is;
		String n = is + "n" + is;
		String o = is + "o" + is;
		String aCol = is + autoIncrementColumn + is;
		// check for delete/update
		if (onColumnsMaxValue != null) {
			onColumnsLastMaxValue = null;
			
			// on join for onColumns only
			StringBuilder onCols = new StringBuilder();
			// on join for all columns except read only
			StringBuilder onAll = new StringBuilder();
			// columns that will be updated
			StringBuilder setColumns = new StringBuilder();
			// distinct onColumns for delete
			StringBuilder selectOnCols = new StringBuilder();
			StringBuilder onSelectOnCols = new StringBuilder();

			// list of used onColumn (@see OnColumnsType)
			List<String> usedOnColumns = new ArrayList<String>();
			for (DbColumnDef colDef : columns.values()) {
				
				IColumn iCol = colDef.getColumn();
				boolean skip = colDef.isReadOnly() || (iCol == null && onColumnsIncludeMissingTargetColumns) || (iCol != null && iCol.isExclude());
				if (!skip && colDef.getName().equalsIgnoreCase(pkColumn != null ? pkColumn : autoIncrementColumn)) {
					skip = true;
				}
				
				if (!skip && onColumnsExclude != null) {
					for (String c : onColumnsExclude) {
						if (c.equalsIgnoreCase(colDef.getName())) {
							skip = true;
							break;
						}
					}
				}
				
				if (skip) {
					continue;
				}
				
				String col = is + colDef.getName() + is;
				String nul = colDef.isAllowsNull() ? getColumnDefaultValueForNullOnJoin(colDef) : null;
				if (onAll.length() > 0) {
					onAll.append(" and ");
				}
				if (nul == null) {
					// default behavior
					onAll.append(n + "." + col + " = " + t + "." + col);
				} else {
					// use coalesce on null values
					onAll.append("coalesce(" + n + "." + col + "," + nul + ")" + " = coalesce(" + t + "." + col + "," + nul + ")");
				}
				
				// onColumns join
				String addOnColumn = null;
				
				if (onColumns.length == 0) {
					// if onColumns is empty use all
					addOnColumn = colDef.getName();
				} else {
					int not_in_list = 0;
					for (String c : onColumns) {
						if (c.equalsIgnoreCase(colDef.getName())) {
							if (onColumnsType == OnColumnsType.UNIQUE) {
								// columns define a unique record
								addOnColumn = colDef.getName();
								break;
							}
						} else {
							// all columns except the onColumns once define a unique record
							not_in_list++;
						}
					}
					if (onColumnsType == OnColumnsType.REVERSE_UNIQUE && not_in_list == onColumns.length) {
						// all columns except the onColumns once define a unique record
						addOnColumn = colDef.getName();;
					}						
				}
				
				if (addOnColumn != null) {
					usedOnColumns.add(addOnColumn);
					if (onCols.length() > 0) {
						onCols.append(" and ");
						onSelectOnCols.append(" and ");
						selectOnCols.append(", ");
					}
					if (nul == null) {
						// default behavior
						onCols.append("" + n + "." + col + " = " + t + "." + col);
						selectOnCols.append(col);
					} else {
						// use coalesce on null values
						onCols.append("coalesce(" + n + "." + col + "," + nul + ")" + " = coalesce(" + t + "." + col + "," + nul + ")");
						selectOnCols.append("coalesce(" + col + "," + nul + ") as " + col);
					}
					onSelectOnCols.append("" + o + "." + col + " = " + t + "." + col);
				} else {
					// add set columns
					if (setColumns.length() > 0) {
						setColumns.append(", ");
					}
					setColumns.append("" + t + "." + col + " = " + n + "." + col + "\n");
				}
			}
			
			class PreparedStatementHelper {
				PreparedStatement getPreparedStatement(String sql, int idCountForColumnsMaxValue) throws SQLException {
					psSql[0] = sql;
					PreparedStatement ps = connection.prepareStatement(sql);
					for (int i = 1; i <= idCountForColumnsMaxValue; i++) {
						ps.setObject(i, onColumnsMaxValue);
					}
					return ps;
				}
			}
			PreparedStatementHelper helper = new PreparedStatementHelper();
			
			String uuid = UUID.randomUUID().toString();
			String 	tempTable = is + "_" + uuid + is; // #temp tables don't survive, don't know why :-(
			String 	tempSql = null;
			int		tempSqlIds = 0;
			String	deleteGoneSql = null;
			int		deleteGoneSqlIds = 0;
			String	deleteSql = null;
			int		deleteSqlIds = 0;
			String	updateSql = null;
			int		updateSqlIds = 0;
			String	deleteUpdatedSql = null;
			int		deleteUpdatedSqlIds = 0;
			String	deleteForReplace = null;
			int		deleteForReplaceIds = 0;
			
			// define sql string value for history start date & time (java now!)
			SimpleDateFormat sqlTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); //2010-10-20 17:34:30.000
			String sql_history_start_now = "'" + sqlTimestampFormat.format(new Date()) + "'";
			String sql_history_end_now = "DateAdd(ms,-2," + sql_history_start_now + ")";
			
			PreparedStatement ps = null;

			String delete_t_from = "delete t from\n";
			String delete_n_from = "delete n from\n";
			String delete_end = "";
			if (sqlDialect == SqlDialect.ORACLE) {
				delete_t_from = "delete (select " + t + ".* from\n";
				delete_n_from = "delete (select " + n + ".* from\n";
				delete_end = ") " + t;
			}
			
			switch (onColumnsMode) {
			case REPLACE_ALL:
				break;
				
			case UPDATE_REMOVE_DELETED:
				// delete old records, that don't exist anymore (IMPORTANT: Data MUST contain complete dataset!)
				deleteGoneSqlIds = 2;
				deleteGoneSql =
					delete_t_from
					+ "(\n"
					+ "	select " + aCol + "\n"
					+ "	, " + selectOnCols + "\n"
					+ "	from "+ getTablenameQuoted() + " t\n"
					+ "	where " + aCol + " <= ?\n"
					+ ") t\n"
					+ "left outer join (\n"
					+ "	select distinct " + selectOnCols + "\n"
					+ "	,1 as " + is + "Exists " + uuid + is + "\n"
					+ "	from " + getTablenameQuoted() + " t\n"
					+ "	where " + aCol + " > ?\n"
					+ ") o on " + onSelectOnCols + "\n"
					+ "where " + o + "." + is + "Exists " + uuid + is + " is null\n"
					+ delete_end;
				
			case UPDATE:
				// delete new records, that already exist
				deleteSqlIds = 2;
				deleteSql =
					  "delete from "+ getTablenameQuoted() + "\n"
					+ "where " + aCol + " in (\n"
					+ "select " + t + "." + aCol + "\n"
					+ "from " + getTablenameQuoted() + " " + t + "\n"
					+ "inner join " + getTablenameQuoted() + " " + n + "\n"
					+ "on " + n + "." + aCol + " <= ?\n"
					+ "and " + onAll + "\n"
					+ "where " + t + "." + aCol + " > ?\n"
					+ ")";
	
				// update old records, that have changed
				updateSqlIds = 2;
				updateSql = setColumns.length() > 0 ?
					  "update t\n"
					+ "set " + setColumns
					+ "from " + getTablenameQuoted() + " t\n"
					+ "inner join " + getTablenameQuoted() + " n\t"
					+ "on " + n + "." + aCol + " > ?\n"
					+ "and " + onCols + "\n"
					+ "where " + t + "." + aCol + " <= ?"
				: null;
				
				// delete new records, that have updated old records
				deleteUpdatedSqlIds = 2;
				deleteUpdatedSql =
					  delete_n_from
					+ getTablenameQuoted() + " t\n"
					+ "inner join " + getTablenameQuoted() + " n\t"
					+ "on " + n + "." + aCol + " > ?\n"
					+ "and " + onCols + "\n"
					+ "where " + t + "." + aCol + " <= ?"
				    + delete_end;

				break;
				
			case REPLACE_NEW:
				// delete new records, that already exist and existing records that are going to be replaced
				
				// identify new records, that already exist
/*
	select n."Id"
	, t."Id" as "Existing Id"
	into "#sales_bookings"
	from "sales_bookings" t with (nolock)
	inner join "sales_bookings" n with (nolock)
	on n."Id" > 17902113 
	--and n."Fiscal Date" = t."Fiscal Date" and n."Sales Rep Participant Key" = t."Sales Rep Participant Key"
	and coalesce(n."Base Booking Flag",'{null:just for join}') = coalesce(t."Base Booking Flag",'{null:just for join}') and coalesce(n."Fiscal Date",'1970-01-01') = coalesce(t."Fiscal Date",'1970-01-01') and coalesce(n."Product Key",-9223372036854775808) = coalesce(t."Product Key",-9223372036854775808) and coalesce(n."Sales Rep Participant Key",-9223372036854775808) = coalesce(t."Sales Rep Participant Key",-9223372036854775808) and coalesce(n."Sales Order Key",-9223372036854775808) = coalesce(t."Sales Order Key",-9223372036854775808) and coalesce(n."Sales Order Line Key",-9223372036854775808) = coalesce(t."Sales Order Line Key",-9223372036854775808) and coalesce(n."Sales District",'{null:just for join}') = coalesce(t."Sales District",'{null:just for join}') and coalesce(n."Sales Team",'{null:just for join}') = coalesce(t."Sales Team",'{null:just for join}') and coalesce(n."Booked Date",'1970-01-01') = coalesce(t."Booked Date",'1970-01-01') and coalesce(n."Sales Order Number",-9223372036854775808) = coalesce(t."Sales Order Number",-9223372036854775808) and coalesce(n."Order Line Number",'{null:just for join}') = coalesce(t."Order Line Number",'{null:just for join}') and coalesce(n."Sales Channel Code",'{null:just for join}') = coalesce(t."Sales Channel Code",'{null:just for join}') and coalesce(n."Sold To Customer NAGP",'{null:just for join}') = coalesce(t."Sold To Customer NAGP",'{null:just for join}') and coalesce(n."Sold To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Sold To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Bill To Customer NAGP",'{null:just for join}') = coalesce(t."Bill To Customer NAGP",'{null:just for join}') and coalesce(n."Bill To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Bill To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Ship To Customer NAGP",'{null:just for join}') = coalesce(t."Ship To Customer NAGP",'{null:just for join}') and coalesce(n."Ship To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Ship To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Account NAGP",'{null:just for join}') = coalesce(t."Account NAGP",'{null:just for join}') and coalesce(n."Account Site CMAT ID",-9223372036854775808) = coalesce(t."Account Site CMAT ID",-9223372036854775808) and coalesce(n."Sold To Partner Site CMAT ID",-9223372036854775808) = coalesce(t."Sold To Partner Site CMAT ID",-9223372036854775808) and coalesce(n."Value Add Partner Site CMAT ID",-9223372036854775808) = coalesce(t."Value Add Partner Site CMAT ID",-9223372036854775808) and coalesce(n."Value Add Partner 2 Site CMAT ID",'{null:just for join}') = coalesce(t."Value Add Partner 2 Site CMAT ID",'{null:just for join}') and coalesce(n."Distributor Site CMAT ID",-9223372036854775808) = coalesce(t."Distributor Site CMAT ID",-9223372036854775808) and coalesce(n."Consolidator Site CMAT ID",'{null:just for join}') = coalesce(t."Consolidator Site CMAT ID",'{null:just for join}') and coalesce(n."SFDC Opportunity Number",'{null:just for join}') = coalesce(t."SFDC Opportunity Number",'{null:just for join}') and coalesce(n."Opportunity ID",'{null:just for join}') = coalesce(t."Opportunity ID",'{null:just for join}') and coalesce(n."System Qty",-9223372036854775808) = coalesce(t."System Qty",-9223372036854775808) and coalesce(n."Controller Qty",-9223372036854775808) = coalesce(t."Controller Qty",-9223372036854775808) and coalesce(n."Storage Capacity GB",-9223372036854775808) = coalesce(t."Storage Capacity GB",-9223372036854775808) and coalesce(n."Booking Qty",-9223372036854775808) = coalesce(t."Booking Qty",-9223372036854775808) and coalesce(n."USD Booking Amount",-9223372036854775808) = coalesce(t."USD Booking Amount",-9223372036854775808) and coalesce(n."USD Extended List Price",-9223372036854775808) = coalesce(t."USD Extended List Price",-9223372036854775808) and coalesce(n."USD Extended Burdened Cost",-9223372036854775808) = coalesce(t."USD Extended Burdened Cost",-9223372036854775808) and coalesce(n."USD Extended Discount",-9223372036854775808) = coalesce(t."USD Extended Discount",-9223372036854775808) and coalesce(n."BE USD Booking Amount",-9223372036854775808) = coalesce(t."BE USD Booking Amount",-9223372036854775808) and coalesce(n."BE USD Extended List Price",-9223372036854775808) = coalesce(t."BE USD Extended List Price",-9223372036854775808) and coalesce(n."BE USD Extended Discount Amount",-9223372036854775808) = coalesce(t."BE USD Extended Discount Amount",-9223372036854775808) and coalesce(n."BE USD Extended Burdened Cost",-9223372036854775808) = coalesce(t."BE USD Extended Burdened Cost",-9223372036854775808) and coalesce(n."GC Booking Amount",-9223372036854775808) = coalesce(t."GC Booking Amount",-9223372036854775808) and coalesce(n."SO Booking Amount",-9223372036854775808) = coalesce(t."SO Booking Amount",-9223372036854775808) and coalesce(n."SO Extended List Price",-9223372036854775808) = coalesce(t."SO Extended List Price",-9223372036854775808) and coalesce(n."SO Extended Burdened Cost",-9223372036854775808) = coalesce(t."SO Extended Burdened Cost",-9223372036854775808) and coalesce(n."SO Extended Discount",-9223372036854775808) = coalesce(t."SO Extended Discount",-9223372036854775808) and coalesce(n."GC Currency Code",'{null:just for join}') = coalesce(t."GC Currency Code",'{null:just for join}') and coalesce(n."SO Extended Burdened Cost2",-9223372036854775808) = coalesce(t."SO Extended Burdened Cost2",-9223372036854775808) and coalesce(n."SO Currency Code",'{null:just for join}') = coalesce(t."SO Currency Code",'{null:just for join}') and coalesce(n."BE Currency Code",'{null:just for join}') = coalesce(t."BE Currency Code",'{null:just for join}') and coalesce(n."SO to GC Conversion Rate",-9223372036854775808) = coalesce(t."SO to GC Conversion Rate",-9223372036854775808) and coalesce(n."BE USD Conversion Rate",-9223372036854775808) = coalesce(t."BE USD Conversion Rate",-9223372036854775808) and coalesce(n."AOP USD Conversion Rate",-9223372036854775808) = coalesce(t."AOP USD Conversion Rate",-9223372036854775808)
	where t."Id" <= 17902113
*/
				tempSqlIds = 2;
				tempSql = 
				  "/* create */ select " + n + "." + aCol + "\n"
				+ ", " + t + "." + aCol + " as " + is + "Existing " + autoIncrementColumn + is + "\n"
				+ "/* into */" + "\n"
				+ "from " + getTablenameQuoted() + " " + t + "\n"
				+ "inner join " +  getTablenameQuoted() + " " + n + "\n"
				+ "on " + n + "." + aCol + " > ?\n"
				+ "and " + onAll + "\n"
				+ "where " + t + "." + aCol + " <= ?\n";
				if (sqlDialect == SqlDialect.MSSQL) {
					tempSql = tempSql.replace("/* into */", "into " + tempTable);
				} else {
					tempSql = tempSql.replace("/* create */", "create table " + tempTable + " as");
				}
				// delete new records, that already exist
/*
delete t
from "sales_bookings" t
inner join "#sales_bookings" n
on t.Id = n.Id
*/
				deleteSqlIds = 0;
				deleteSql =
				  "delete " + t + "\n"
				+ "from " + getTablenameQuoted() + " " + t + "\n"
				+ "where " + t + "." + aCol + " in (select " + aCol + " from " + tempTable + ")\n";
				
				// delete existing records for replace (excl. the once that should remain)

/*
delete t
from (
	select "Id"
	, coalesce("Fiscal Date",'1970-01-01') as "Fiscal Date", coalesce("Sales Rep Participant Key",-9223372036854775808) as "Sales Rep Participant Key"
	from "sales_bookings" t with (nolock)
	where "Id" <= 18002113
) t
inner join (
	select distinct coalesce("Fiscal Date",'1970-01-01') as "Fiscal Date", coalesce("Sales Rep Participant Key",-9223372036854775808) as "Sales Rep Participant Key"
	from "sales_bookings" t with (nolock)
	where "Id" > 18002113
) o on o."Fiscal Date" = t."Fiscal Date" and o."Sales Rep Participant Key" = t."Sales Rep Participant Key"
left outer join "#sales_bookings" n
on n."Existing Id" = t.Id
where n.[Existing Id] is null
*/
				deleteForReplaceIds = 2;
				deleteForReplace =
				  "delete from "+ getTablenameQuoted() + "\n"
				+ "where " + aCol + " in (\n"
				+ "select " + t + "." + aCol + " from (\n"
				+ "	select " + aCol + "\n"
				+ "	, " + selectOnCols + "\n"
				+ "	from "+ getTablenameQuoted() + " " + t + "\n"
				+ "	where " + aCol + " <= ?\n"
				+ ") " + t + "\n"
				+ "inner join (\n"
				+ "	select distinct " + selectOnCols + "\n"
				+ "	from " + getTablenameQuoted() + " " + t + "\n"
				+ "	where " + aCol + " > ?\n"
				+ ") " + o + " on " + onSelectOnCols + "\n"
				+ "left outer join " + tempTable + " " + n + "\n"
				+ "on " + n + "." + is + "Existing " + autoIncrementColumn + is + " = " + t + "." + aCol + "\n"
				+ "where " + n + "." + is + "Existing " + autoIncrementColumn + is + " is null"
				+ ")";
				
				break;
				
			/** Maintains history of uniquely identified records (incl. history start and end date)!
			 *  Delete new records, that already exist for current record (history end date = 9999-31-12).
			 *  Update old records' history end date, that have changed or got deleted (use history deleted date column
			 *  when source does not contain complete data set!).
			 *  Delete new records, that have updated old records.
			 *  (when source is incremental and records get deleted the source requires a history deleted date column with
			 *  date(/time) of the deletion in database time-zone, or the source contains complete data set!)
			 */
			case REPLACE_NEW_MAINTAIN_HISTORY: /* IMPORTANT: This mode is still not ready and under development ! ! ! ! ! ! ! */
/*
	select n."Id"
	, t."Id" as "Existing Id"
	into "#sales_bookings"
	from "sales_bookings" t with (nolock)
	inner join "sales_bookings" n with (nolock)
	on n."Id" > 17902113 
	--and n."Fiscal Date" = t."Fiscal Date" and n."Sales Rep Participant Key" = t."Sales Rep Participant Key"
	and coalesce(n."Base Booking Flag",'{null:just for join}') = coalesce(t."Base Booking Flag",'{null:just for join}') and coalesce(n."Fiscal Date",'1970-01-01') = coalesce(t."Fiscal Date",'1970-01-01') and coalesce(n."Product Key",-9223372036854775808) = coalesce(t."Product Key",-9223372036854775808) and coalesce(n."Sales Rep Participant Key",-9223372036854775808) = coalesce(t."Sales Rep Participant Key",-9223372036854775808) and coalesce(n."Sales Order Key",-9223372036854775808) = coalesce(t."Sales Order Key",-9223372036854775808) and coalesce(n."Sales Order Line Key",-9223372036854775808) = coalesce(t."Sales Order Line Key",-9223372036854775808) and coalesce(n."Sales District",'{null:just for join}') = coalesce(t."Sales District",'{null:just for join}') and coalesce(n."Sales Team",'{null:just for join}') = coalesce(t."Sales Team",'{null:just for join}') and coalesce(n."Booked Date",'1970-01-01') = coalesce(t."Booked Date",'1970-01-01') and coalesce(n."Sales Order Number",-9223372036854775808) = coalesce(t."Sales Order Number",-9223372036854775808) and coalesce(n."Order Line Number",'{null:just for join}') = coalesce(t."Order Line Number",'{null:just for join}') and coalesce(n."Sales Channel Code",'{null:just for join}') = coalesce(t."Sales Channel Code",'{null:just for join}') and coalesce(n."Sold To Customer NAGP",'{null:just for join}') = coalesce(t."Sold To Customer NAGP",'{null:just for join}') and coalesce(n."Sold To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Sold To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Bill To Customer NAGP",'{null:just for join}') = coalesce(t."Bill To Customer NAGP",'{null:just for join}') and coalesce(n."Bill To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Bill To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Ship To Customer NAGP",'{null:just for join}') = coalesce(t."Ship To Customer NAGP",'{null:just for join}') and coalesce(n."Ship To Customer Company CMAT ID",-9223372036854775808) = coalesce(t."Ship To Customer Company CMAT ID",-9223372036854775808) and coalesce(n."Account NAGP",'{null:just for join}') = coalesce(t."Account NAGP",'{null:just for join}') and coalesce(n."Account Site CMAT ID",-9223372036854775808) = coalesce(t."Account Site CMAT ID",-9223372036854775808) and coalesce(n."Sold To Partner Site CMAT ID",-9223372036854775808) = coalesce(t."Sold To Partner Site CMAT ID",-9223372036854775808) and coalesce(n."Value Add Partner Site CMAT ID",-9223372036854775808) = coalesce(t."Value Add Partner Site CMAT ID",-9223372036854775808) and coalesce(n."Value Add Partner 2 Site CMAT ID",'{null:just for join}') = coalesce(t."Value Add Partner 2 Site CMAT ID",'{null:just for join}') and coalesce(n."Distributor Site CMAT ID",-9223372036854775808) = coalesce(t."Distributor Site CMAT ID",-9223372036854775808) and coalesce(n."Consolidator Site CMAT ID",'{null:just for join}') = coalesce(t."Consolidator Site CMAT ID",'{null:just for join}') and coalesce(n."SFDC Opportunity Number",'{null:just for join}') = coalesce(t."SFDC Opportunity Number",'{null:just for join}') and coalesce(n."Opportunity ID",'{null:just for join}') = coalesce(t."Opportunity ID",'{null:just for join}') and coalesce(n."System Qty",-9223372036854775808) = coalesce(t."System Qty",-9223372036854775808) and coalesce(n."Controller Qty",-9223372036854775808) = coalesce(t."Controller Qty",-9223372036854775808) and coalesce(n."Storage Capacity GB",-9223372036854775808) = coalesce(t."Storage Capacity GB",-9223372036854775808) and coalesce(n."Booking Qty",-9223372036854775808) = coalesce(t."Booking Qty",-9223372036854775808) and coalesce(n."USD Booking Amount",-9223372036854775808) = coalesce(t."USD Booking Amount",-9223372036854775808) and coalesce(n."USD Extended List Price",-9223372036854775808) = coalesce(t."USD Extended List Price",-9223372036854775808) and coalesce(n."USD Extended Burdened Cost",-9223372036854775808) = coalesce(t."USD Extended Burdened Cost",-9223372036854775808) and coalesce(n."USD Extended Discount",-9223372036854775808) = coalesce(t."USD Extended Discount",-9223372036854775808) and coalesce(n."BE USD Booking Amount",-9223372036854775808) = coalesce(t."BE USD Booking Amount",-9223372036854775808) and coalesce(n."BE USD Extended List Price",-9223372036854775808) = coalesce(t."BE USD Extended List Price",-9223372036854775808) and coalesce(n."BE USD Extended Discount Amount",-9223372036854775808) = coalesce(t."BE USD Extended Discount Amount",-9223372036854775808) and coalesce(n."BE USD Extended Burdened Cost",-9223372036854775808) = coalesce(t."BE USD Extended Burdened Cost",-9223372036854775808) and coalesce(n."GC Booking Amount",-9223372036854775808) = coalesce(t."GC Booking Amount",-9223372036854775808) and coalesce(n."SO Booking Amount",-9223372036854775808) = coalesce(t."SO Booking Amount",-9223372036854775808) and coalesce(n."SO Extended List Price",-9223372036854775808) = coalesce(t."SO Extended List Price",-9223372036854775808) and coalesce(n."SO Extended Burdened Cost",-9223372036854775808) = coalesce(t."SO Extended Burdened Cost",-9223372036854775808) and coalesce(n."SO Extended Discount",-9223372036854775808) = coalesce(t."SO Extended Discount",-9223372036854775808) and coalesce(n."GC Currency Code",'{null:just for join}') = coalesce(t."GC Currency Code",'{null:just for join}') and coalesce(n."SO Extended Burdened Cost2",-9223372036854775808) = coalesce(t."SO Extended Burdened Cost2",-9223372036854775808) and coalesce(n."SO Currency Code",'{null:just for join}') = coalesce(t."SO Currency Code",'{null:just for join}') and coalesce(n."BE Currency Code",'{null:just for join}') = coalesce(t."BE Currency Code",'{null:just for join}') and coalesce(n."SO to GC Conversion Rate",-9223372036854775808) = coalesce(t."SO to GC Conversion Rate",-9223372036854775808) and coalesce(n."BE USD Conversion Rate",-9223372036854775808) = coalesce(t."BE USD Conversion Rate",-9223372036854775808) and coalesce(n."AOP USD Conversion Rate",-9223372036854775808) = coalesce(t."AOP USD Conversion Rate",-9223372036854775808)
	where t."Id" <= 17902113
*/

				// identify new records, that already exist for current record (history end date = 9999-31-12).
				tempSqlIds = 2;
				tempSql = 
				  "select " + n + "." + aCol + "\n"
				+ ", " + t + "." + aCol + " as " + is + "Existing " + autoIncrementColumn + is + "\n"
				+ "into " + tempTable + "\n"
				+ "from " + getTablenameQuoted() + " t\n"
				+ "inner join " +  getTablenameQuoted() + " n\n"
				+ "on " + n + "." + aCol + " > ?\n"
				+ "and " + onAll + "\n"
				+ "where " + t + "." + aCol + " <= ?\n and " + is + onColumnsHistoryEndDate + is + " = '9999-12-31'";
				
/*
delete t
from "sales_bookings" t
inner join "#sales_bookings" n
on t.Id = n.Id
*/

				// Delete new records, that already exist for current record (history end date = 9999-31-12).
				deleteSqlIds = 0;
				deleteSql =
				  delete_t_from
				+ getTablenameQuoted() + " t\n"
				+ "inner join " + tempTable + " n\n"
				+ "on " + n + "." + aCol + " = " + t + "." + aCol
				+ delete_end;
				
				/** Update old records' history end date, that have changed or got deleted (use history deleted date column
				 *  when source does not contain complete data set!).
				 */

/*
delete t
from (
	select "Id"
	, coalesce("Fiscal Date",'1970-01-01') as "Fiscal Date", coalesce("Sales Rep Participant Key",-9223372036854775808) as "Sales Rep Participant Key"
	from "sales_bookings" t with (nolock)
	where "Id" <= 18002113
) t
inner join (
	select distinct coalesce("Fiscal Date",'1970-01-01') as "Fiscal Date", coalesce("Sales Rep Participant Key",-9223372036854775808) as "Sales Rep Participant Key"
	from "sales_bookings" t with (nolock)
	where "Id" > 18002113
) o on o."Fiscal Date" = t."Fiscal Date" and o."Sales Rep Participant Key" = t."Sales Rep Participant Key"
left outer join "#sales_bookings" n
on n."Existing Id" = t.Id
where n.[Existing Id] is null
*/

				// Update old records' history end date, that have changed or got deleted
				deleteForReplaceIds = 2;
				if (onColumnsHistoryDeletedDate == null) {
					// no history deleted date available
					deleteForReplace =
						  "update t\n"
						+ "set " + is + onColumnsHistoryEndDate + is + " = " + sql_history_end_now
						+ "from (\n"
						+ "	select " + aCol + "\n"
						+ "	, " + selectOnCols + "\n"
						+ "	from "+ getTablenameQuoted() + " t\n"
						+ "	where " + aCol + " <= ? and " + is + onColumnsHistoryEndDate + is + " = '9999-12-31'\n"
						+ ") t\n"
						+ "inner join (\n"
						+ "	select distinct " + selectOnCols + "\n"
						+ "	from " + getTablenameQuoted() + " t\n"
						+ "	where " + aCol + " > ?\n"
						+ ") o on " + onSelectOnCols + "\n"
						+ "left outer join " + tempTable + " n\n"
						+ "on " + n + "." + is + "Existing " + autoIncrementColumn + is + " = " + t + "." + aCol + "\n"
						+ "where " + n + "." + is + "Existing " + autoIncrementColumn + is + " is null";
				} else {
					// use history deleted date where available
					deleteForReplace =
						  "update t\n"
						+ "set " + is + onColumnsHistoryEndDate + is + " = coalesce(" + o + "." + is + onColumnsHistoryDeletedDate + is + "," + sql_history_end_now + ")\n"
						+ "from (\n"
						+ "	select " + aCol + "\n"
						+ "	, " + selectOnCols + "\n"
						+ "	from "+ getTablenameQuoted() + " t\n"
						+ "	where " + aCol + " <= ? and " + is + onColumnsHistoryEndDate + is + " = '9999-12-31'\n"
						+ ") t\n"
						+ "inner join (\n"
						+ "	select distinct " + selectOnCols + "\n"
						+ " , " + is + onColumnsHistoryDeletedDate + is + "\n"
						+ " , " + is + onColumnsHistoryStartDate + is + "\n"
						+ "	from " + getTablenameQuoted() + " t\n"
						+ "	where " + aCol + " > ?\n"
						+ ") o on " + onSelectOnCols + "\n"
						+ "left outer join " + tempTable + " n\n"
						+ "on " + n + "." + is + "Existing " + autoIncrementColumn + is + " = " + t + "." + aCol + "\n"
						+ "where " + n + "." + is + "Existing " + autoIncrementColumn + is + " is null";
				}
				
				break;
			}
			
			try {
				switch (onColumnsMode) {
				case REPLACE_ALL:
					break;
					
				case UPDATE_REMOVE_DELETED: {
					// first remove data that don't exists anymore
					ps = helper.getPreparedStatement(deleteGoneSql, deleteGoneSqlIds);
					monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for old (removed) records"); 
					int deleted = ps.executeUpdate();
					monitor.logInfo(this + " " + deleted + " records deleted.");
					ps.close();
					
					// update delete count
					deleteCount += deleted;
				}
				case UPDATE: {
					
					ps = helper.getPreparedStatement(deleteSql, deleteSqlIds);
					monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for new records"); 
					int deleted = ps.executeUpdate();
					monitor.logInfo(this + " " + deleted + " records deleted.");
					
					if (updateSql != null) {
						ps.close();
						
						ps = helper.getPreparedStatement(updateSql, updateSqlIds);
						monitor.logInfo(this + " executes update statement on table " + getTablenameQuoted() + " for existing records"); 
						int updated = ps.executeUpdate();
						monitor.logInfo(this + " " + updated + " existing records updated.");
						
						int deletedUpdated = 0;
						if (updated > 0) {
							ps.close();
							
							ps = helper.getPreparedStatement(deleteUpdatedSql, deleteUpdatedSqlIds);
							monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for new records that updated existing records"); 
							deletedUpdated = ps.executeUpdate();
							monitor.logInfo(this + " " + deletedUpdated + " new records deleted that updated existing records.");
						}
						if (deletedUpdated != updated) {
							// this should not occur as the onColumns should identify uniquely the records
							throw new SQLException(
								"Updated and deleted counts don't match, data wouldn't be consistent! " +
								"Please validate unique record identification in table " + getTablenameQuoted() + " on columns " + usedOnColumns);
						}

						// update inserted and updated counts
						insertCount -= deletedUpdated;
						updateCount += updated;
					}
					
					// update inserted count
					insertCount -= deleted;
					
					break;
				}
				case REPLACE_NEW: {
					
					ps = helper.getPreparedStatement(tempSql, tempSqlIds);
					monitor.logInfo(this + " executes identification statement on table " + getTablenameQuoted() + " for new records"); 
					int existing = ps.executeUpdate();
					int deleted = 0;
					int deleted_existing = 0;
					monitor.logInfo(this + " identified " + existing + " existing records.");
					
					if (existing > 0) {
						ps.close();

						ps = helper.getPreparedStatement(deleteSql, deleteSqlIds);
						monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for new records"); 
						deleted = ps.executeUpdate();
						monitor.logInfo(this + " " + deleted + " records deleted.");
						ps.close();
					}

					ps = helper.getPreparedStatement(deleteForReplace, deleteForReplaceIds);
					monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for existing records"); 
					deleted_existing = ps.executeUpdate();
					monitor.logInfo(this + " " + deleted_existing + " records deleted.");

					// update inserted and deleted counts
					insertCount -= deleted;
					deleteCount += deleted_existing;
					
					break;
				}
				case REPLACE_NEW_MAINTAIN_HISTORY: {
					
					ps = helper.getPreparedStatement(tempSql, tempSqlIds);
					monitor.logInfo(this + " executes identification statement on table " + getTablenameQuoted() + " for new records"); 
					int existing = ps.executeUpdate();
					int deleted = 0;
					int updated_existing = 0;
					monitor.logInfo(this + " identified " + existing + " existing records.");
					
					if (existing > 0) {
						ps.close();

						ps = helper.getPreparedStatement(deleteSql, deleteSqlIds);
						monitor.logInfo(this + " executes delete statement on table " + getTablenameQuoted() + " for new records"); 
						deleted = ps.executeUpdate();
						monitor.logInfo(this + " " + deleted + " records deleted.");
						ps.close();
					}

					ps = helper.getPreparedStatement(deleteForReplace, deleteForReplaceIds);
					monitor.logInfo(this + " executes history end date update statement on table " + getTablenameQuoted() + " for existing records"); 
					updated_existing = ps.executeUpdate();
					monitor.logInfo(this + " " + updated_existing + " records updated.");

					// update inserted and deleted counts
					insertCount -= deleted;
					updateCount += updated_existing;
					
					break;
				}
				}
				if (ps != null) {
					PreparedStatement ps2 = ps;
					ps = null;
					ps2.close();
				}
			} finally {
				try {
					onColumnsLastMaxValue = onColumnsMaxValue;
					// clear max id
					onColumnsMaxValue = null;
					if (ps != null) {
						monitor.logError("Error executing prepared statement: " + psSql[0]);
						ps.close();
					}
				} finally {
					if (tempSql != null) {
						// remove temp table
						ps = connection.prepareStatement("drop table " + tempTable);
						ps.execute();
						ps.close();
					}
				}
			}
		}
	}

	/**
	 * @param colDef
	 * @return
	 * @throws SQLException 
	 */
	protected String getColumnDefaultValueForNullOnJoin(DbColumnDef colDef) throws SQLException {
		if (colDef.isAllowsNull()) {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CHAR:
			case Types.CLOB:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			case Types.NCHAR:
			case Types.NCLOB:
				return "'{null:just for join}'";
				
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			case Types.BIT:
			case Types.BOOLEAN:
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				return "-9223372036854775808"; // MS SQL min BIGINT
			
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return "'1970-01-01'";
				
			default:
				throw new SQLException("Unsupport java.sql.Types " + colDef.getType() + " in column " + colDef); 
			}
		}
		return null;
	}

	/**
	 * @param colDef
	 * @return
	 * @throws SQLException
	 */
	protected String getColumnDefaultValueForNull(DbColumnDef colDef) throws SQLException {
		if (!colDef.isAllowsNull()) {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CHAR:
			case Types.CLOB:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			case Types.NCHAR:
			case Types.NCLOB:
				return "";
				
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			case Types.BIT:
			case Types.BOOLEAN:
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				return "0";
			
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return "1970-01-01";
				
			default:
				throw new SQLException("Unsupport java.sql.Types " + colDef.getType() + " in column " + colDef); 
			}
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void preSourceProcessing(IProcessContext context) throws ETLException {
		super.preSourceProcessing(context);
		
		// check target table
		if (tablename == null) {
			throw new ETLException("Tablename not specified.");
		}
		
		// does the table exist?
		try {
			if (originalTablename == null) {
				originalTablename = tablename;
			}
			
			// check table structure, adds missing columns
			checkTableStructure(true);
			
			if (truncateTable && !tablePurged) {
				// truncate table only once for source processing, set to false in method initialize
				truncateTable();
			}
			
			if (deleteTable && !tablePurged) {
				// delete from table only once for source processing, set to false in method initialize
				deleteTable();
			}
			
			// build prepared statement.. and Update statement.
			buildPreparedStatements(false);
			
			// set offset for batch insert/update
			batchRecordsCountOffset = context.getRecordsCount();
			
		} catch (SQLException se) {
			throw new ETLException("Error initializing target database/tables: " + se, se);
		}
	}
	
	protected String buildPreparedStatement(String tablename, Collection<DbColumnDef> columns, Mode mode, String pkColumn, String pkSequence) {
		
		boolean simulatePkIdentity = isSimulatePkIdentity();
		
		StringBuilder sql = new StringBuilder();
		StringBuilder sqlValues = new StringBuilder();
		
		boolean insert = true;
		String command = "INSERT INTO ";
		if (mode == Mode.UPDATE) {
			insert = false;
			command = "UPDATE ";
		}
		
		sql.append(command + getTablenameQuoted(tablename));
		if (withTablock) {
			sql.append(" WITH (TABLOCK)");
		}
		if (insert) {
			sql.append(" (");
		} else {
			sql.append(" SET ");
		}
		sqlValues.append("(");
		
		boolean first = true;
		for (DbColumnDef colDef : columns) {
			
			if (colDef.getColumn() != null 
					|| (simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn)) 
					|| (/* support preventNotNullError in insert mode */ insert && preventNotNullError && !colDef.isAllowsNull())) {
				
				// ignore readonly columns for insert and update (identity is not support on ORACLE)
				if (colDef.isReadOnly() || (!simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn))) {
					continue;
				}
				
				if (insert) {
					// INSERT Statement
					if (first) {
						first = false;
					} else {
						sql.append(", ");
						sqlValues.append(", ");
					}
					sql.append(is + colDef.getName() + is);
					if (simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn)) {
						sqlValues.append(is + pkSequence + is + ".NEXTVAL");
					} else {
						sqlValues.append("?");
					}
				} else {
					// UPDATE Statement (might skip pk)
					if (!colDef.getName().equalsIgnoreCase(pkColumn)) {
						if (first) {
							first = false;
						} else {
							sql.append(", ");
						}
						sql.append(is + colDef.getName() + is + " = ?");
					}
				}
			}
		}
		
		if (insert) {
			
			sqlValues.append(")");
			sql.append(") VALUES" + sqlValues);
			
		} else {		
			
			sql.append(" WHERE " + is + pkColumn + is + " = ?");
		}
		
		return sql.toString();
	}
	
	/**
	 * Build prepared SQL statements
	 * @throws SQLException 
	 */
	protected void buildPreparedStatements(boolean refresh) throws SQLException {
		String sql = buildPreparedStatement(tablename, columns.values(), Mode.INSERT, pkColumn != null ? pkColumn : autoIncrementColumn, pkSequence); 
		String sqlUpd = buildPreparedStatement(tablename, columns.values(), Mode.UPDATE, pkColumn, null);
		
		if (!refresh) {
			for (DbColumnDef colDef : columns.values()) {
	
				if (colDef.getColumn() == null) {
					if (!ignoreMissingSourceColumns && !ignoredColumns.contains(colDef.getName()) && !colDef.isReadOnly()) {
						monitor.logWarn("A column in the target table does not exist in the source and is skipped (" + colDef.getName() + ")");
					}
				}
			}
		} else {
			monitor.logWarn("Rebuild Prepared Statement: " + sql);
		}
		
		if (mode == Mode.INSERT || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("INSERT Statement: " + sql);
			psInsert = connection.prepareStatement(sql /*, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY*/);
		}
		
		if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("UPDATE Statement: " + sqlUpd);
			psUpdate = connection.prepareStatement(sqlUpd /*, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY*/);
		}
		
		// added columnAddedListener
		if (columnAddedListener == null) {
			columnAddedListener = new IDataSetColumnAdded() {
				@Override
				public void onDataSetColumnAdded(IDataSet dataSet, IColumn column) throws ETLException {
					processContext.getMonitor().logInfo("Add new column '" + column.getName() + "' to JDBCLoader on table '" + tablename + "'");
					executeBatch();
					try {
						if (psInsert != null) {
							psInsert.close();
							psInsert = null;
						}
						if (psUpdate != null) {
							psUpdate.close();
							psUpdate = null;
						}
						checkTableStructure(false);
						buildPreparedStatements(true);
					} catch (SQLException e) {
						throw new ETLException(e);
					}
				}
			};
			processContext.getDataSet().addOnDataSetColumnAdded(columnAddedListener);
		}
	}

	/**
	 * @return count of rows or null if table doesn't exist 
	 */
	public Integer getTableRowCount() throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet rs = metaData.getTables(catalogname == null ? connection.getCatalog() : catalogname, schemaname, tablename, null);
		try {
			if (!rs.next()) {
				// table doesn't exist
				return null;
			}
		} finally {
			rs.close();
		}

		// get count of rows
		Statement stmt = connection.createStatement();
		try {
			rs = JDBCUtil.executeQuery(stmt, "SELECT COUNT(*) FROM " + getTablenameQuoted());
			rs.next();
			return rs.getInt(1);
		} finally {
			rs.close();
			stmt.close();
		}

	}
	
	/**
	 * Checks if table exists and auto creates it if autoCreateTable is true.
	 * @return DatabaseMetaData
	 * @throws SQLException 
	 * @throws ETLException
	 */
	protected synchronized DatabaseMetaData checkTableExists() throws SQLException, ETLException {
		String aCol = is + autoIncrementColumn + is;
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet rs = metaData.getTables(catalogname == null ? connection.getCatalog() : catalogname, schemaname, tablename, null);
		try {
			if (!rs.next()) {
				if (!autoCreateTable) {
					throw new ETLException("The target table " + getTablenameQuoted() + " does not exist.");
				}
				
				List<DbColumnDef> dbColumnDefs = new ArrayList<DbColumnDef>();
				
				// Check if columns can be created immediately
				if (processContext.getCurrentSource() instanceof JDBCSource) {
					for (IColumn column : processContext.getDataSet().getColumns()) {
						
						// exclude "" empty columns
						if (column.computeTargetName() == null || column.computeTargetName().isEmpty()) {
							continue;
						}
						
						if (excludedColumns.contains(column.getName())) {
							continue;
						}
						
						if (!column.isExclude()) {
							DbColumnDef dbcd = getDbColumnDef(tablename, column);
							dbColumnDefs.add(dbcd);
						}
					}
				}
				
				createTable(tablename, dbColumnDefs);

			} else {
				if (pkSequence == null && isSimulatePkIdentity()) {
					pkSequence = getObjectAlias(null, "SEQ_" + originalTablename);
				}
				// table existed, check if incremental replace is enabled for INSERT mode
				if (mode == Mode.INSERT && (!onColumnsOnProcessFinished || onColumnsMaxValue == null)) {
					// get max auto increment id for replace
					Statement stmt = connection.createStatement();
					try {
						ResultSet rs_max = JDBCUtil.executeQuery(stmt, "select max(" + aCol + ") from " + getTablenameQuoted());
						try {
							rs_max.next();
							onColumnsMaxValue = rs_max.getObject(1);
						} finally {
							rs_max.close();
						}
					} finally {
						stmt.close();
					}
					if (onColumnsMaxValue != null) {
						StringBuilder cols = new StringBuilder();
						if (onColumns != null) {
							for (String col : onColumns) {
								if (cols.length() > 0) {
									cols.append(", ");
								}
								cols.append(is + col + is);
							}
						}
						if (onColumns == null) {
							monitor.logInfo("Current max(" + aCol + ") = " + onColumnsMaxValue);
						} else if (onColumns.length == 0) {
							monitor.logInfo("Using max(" + aCol + ") = " + onColumnsMaxValue + " to replace records on all columns");
						} else {
							switch (onColumnsType) {
							case UNIQUE:
								monitor.logInfo("Using max(" + aCol + ") = " + onColumnsMaxValue + " to replace records on columns " + cols);
								break;
							case REVERSE_UNIQUE:
								monitor.logInfo("Using max(" + aCol + ") = " + onColumnsMaxValue + " to replace records on any column except " + cols);
								break;
							}
						}
					}
				}
			}
		} finally {
			rs.close();
		}
		return metaData;
	}
	
	/**
	 * Checks table structure and creates missing columns if autoCreateColumns is enabled
	 * @throws ETLException 
	 * @throws SQLException 
	 * 
	 */
	protected void checkTableStructure(boolean autoDetectColumnTypesAllowed) throws ETLException, SQLException {
		
		String lock = connectionName + ":" + getTablenameQuoted();
		synchronized (SYNCHRONIZE_TABLE_ACCESS) {
			String synchronizedLock = SYNCHRONIZE_TABLE_ACCESS.get(lock);
			if (synchronizedLock == null) {
				SYNCHRONIZE_TABLE_ACCESS.put(lock, lock);
			} else {
				lock = synchronizedLock;
			}
		}
		
		synchronized (lock) {
			
			tablename = getObjectAlias(null, tablename);
			if (originalTablename != null && !originalTablename.equals(tablename)) {
				monitor.logWarn(this + " uses tablename alias " + is + tablename + is + " for originally configured tablename " + is + originalTablename + is);
			}
			
			DatabaseMetaData metaData = checkTableExists();
	
			columns = loadColumns(metaData, tablename);
			
			List<IColumn> missingCols = new ArrayList<IColumn>();
			
			// Check if the columns apply.
			for (IColumn column : processContext.getDataSet().getColumns()) {
				
				// exclude "" empty columns
				if (column.computeTargetName() == null || column.computeTargetName().isEmpty()) {
					column.setExclude(true);
				}
				
				if (excludedColumns.contains(column.getName())) {
					continue;
				}
				
				DbColumnDef dbcd = columns.get(column.computeTargetName().toUpperCase());
				if (dbcd == null) {
					// try column name
					dbcd = columns.get(column.getName().toUpperCase());
				}
				if (!column.isExclude()) {
					if (dbcd == null) {
						// try column alias
						String originalColumnName = column.computeTargetName();
						dbcd = columns.get(getObjectAlias(originalTablename, originalColumnName).toUpperCase());
						if (dbcd == null) {
							// try column name
							originalColumnName = column.getName();
							dbcd = columns.get(getObjectAlias(originalTablename, originalColumnName).toUpperCase());
						}
						if (dbcd != null) {
							//monitor.logWarn(this + " uses column alias " + is + dbcd.getName() + is + " for originally configured column " + is + originalColumnName + is);
						}
					}
					if (dbcd != null) {
						dbcd.setColumn(column);
					} else {
						processContext.getMonitor().logWarn("Column does not exist: " + is + column.computeTargetName() + is);
						missingCols.add(column);
					}
				} else if (dbcd != null) {
					// ignore this warning on excluded columns: A column in the target table does not exist in the source and is skipped
					addIgnoreableColumns(dbcd.getName());
				}
			}
			if (addColumnsToDataSet) {
				// add additional columns in table to dataset
				for (DbColumnDef dbCol : columns.values()) {
					String colName = dbCol.getName();
					if (dbCol.getColumn() == null && !dbCol.isReadOnly() && !processContext.getDataSet().containsColumn(colName)) {
						//context.getMonitor().logInfo("Adding table column '" + colName + "' to DataSet");
						IColumn col = new Column(colName);
						int type = dbCol.getType();
						int precision = dbCol.getSize();
						int scale = dbCol.getScale();
						JDBCUtil.updateColumn(col, type, precision, scale);
						processContext.getDataSet().addColumn(col);
						dbCol.setColumn(col);
					}
				}
			}
			if (missingCols.size() > 0) {
				if (autoCreateColumns) {
					if (autoDetectColumnTypesAllowed && autoDetectColumnTypes && !(processContext.getCurrentSource() instanceof JDBCSource)) {
						// auto-detect column types for missing columns
						autoDetectColumnTypes(missingCols);
					}
					createColumns(missingCols, columns);
				} else {
					if (!ignoreMissingTargetColumns) {
						throw new ETLException("The source contains columns that do not exist in the target table.");
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param metaData
	 * @param tablename
	 * @return
	 * @throws SQLException 
	 */
	protected Map<String, DbColumnDef> loadColumns(DatabaseMetaData metaData, String tablename) throws SQLException {
		return JDBCUtil.getColumns(metaData, schemaname, tablename, true);
	}

	/**
	 * Do an addition full source scan of value analysis.
	 * @param missingCols
	 * @throws ETLException 
	 */
	protected void autoDetectColumnTypes(List<IColumn> missingCols) throws ETLException {
		autoDetectColumnTypesRunning = true;
		try {
			IETLProcess process = (IETLProcess)processContext.getProcess();
			
			process.getMonitor().logInfo("Auto detect missing columns " + missingCols);
			
			IExtractor extractor = process.getExtractor();
	
			class ColumnType {
				boolean isInteger = true;
				boolean isLong = true;
				boolean isDouble = true;
				boolean isDate = true;
				boolean isBoolean = true;
				int maxLength = 0;
				int count = 0;
				SimpleDateFormat dateFormat = null;
			}
			
			Map<IColumn, ColumnType> columnTypes = new HashMap<IColumn, ColumnType>();
			
			SimpleDateFormat[] dateFormat = new SimpleDateFormat[] {
				null, // place holder for ColumnType.dateFormat
				new SimpleDateFormat("MM/dd/yyyy hh:mm aa"),
				new SimpleDateFormat("MM/dd/yyyy"),
				new SimpleDateFormat("dd-MMM-yyyy"),
				new SimpleDateFormat("MM/dd/yy"),
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S"),
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
				new SimpleDateFormat("yyyy-MM-dd"),
			};
			
			// assume source is opened already, iterate all records
			Calendar cal = Calendar.getInstance();
			IRecord record;
			while ((record = extractor.getNextRecord()) != null) {
	
				// skip invalid records
				if (record.isInvalid()) {
					continue;
				}
				
				// invoke transformers
				for (ITransformer transformer : process.getTransformers()) {
					transformer.processRecord(processContext, record);
				}
				
				// iterate missing columns
				for (IColumn column : missingCols) {
					
					ColumnType columnType = columnTypes.get(column);
					if (columnType == null) {
						columnType = new ColumnType();
						columnTypes.put(column, columnType);
					}
					
					Object value = record.getData(column);
					if (value == null) {
						continue;
					}
					
					// identify type
					Number n = null;
					Date d = null;
					String s = null;
					Boolean b = null;
					if (value instanceof String) {
						s = (String)value;
					} else if (value instanceof Number) {
						n = (Number)value;
					} else if (value instanceof Date) {
						d = (Date)value;
					} else if (value instanceof Boolean) {
						b = (Boolean)value;
					} else {
						s = value.toString();
					}
					if (s != null && treatEmptyAsNull && s.length() == 0) {
						continue;
					}
					
					if (s != null) {
						// check integer
						if (columnType.isInteger) {
							try {
								n = Integer.parseInt(s);
								if (s.startsWith("0") && s.length() > 1) {
									columnType.isInteger = false;
								}
								// check range
								/*if (n.intValue() < INT_RANGE[0] || n.intValue() > INT_RANGE[1]) {
									columnType.isInteger = false;
								}*/
							} catch (Exception e) {
								columnType.isInteger = false;
							}
						}
						// check long
						if (columnType.isLong) {
							try {
								n = Long.parseLong(s);
								if (s.startsWith("0") && s.length() > 1) {
									columnType.isLong = false;
								}
								// check range
								/*if (n.longValue() < BIGINT_RANGE[0] || n.longValue() > BIGINT_RANGE[1]) {
									columnType.isLong = false;
								}*/
							} catch (Exception e) {
								columnType.isLong = false;
							}
						}
						// check double
						if (columnType.isDouble) {
							try {
								n = Double.parseDouble(s);
								if (s.startsWith("0") && !s.startsWith("0.") && s.length() > 1) {
									columnType.isDouble = false;
								}
							} catch (Exception e) {
								columnType.isDouble = false;
							}
						}
						// check date
						if (columnType.isDate) {
							boolean isDate = false;
							for (SimpleDateFormat format : dateFormat) {
								try {
									if (format == null) {
										format = columnType.dateFormat;
									}
									if (format == null) {
										continue;
									}
									Date date = format.parse(s);
									cal.setTime(date);
									String test = format.format(date);
									if (cal.get(Calendar.YEAR) >= 1969 && cal.get(Calendar.YEAR) < 2100 && test.length() >= s.length() && test.length() <= s.length() + 2) {
										columnType.dateFormat = format;
										isDate = true;
									}
									break;
								} catch (Exception e) {}
							}
							if (!isDate) {
								columnType.isDate = false;
							}
						}
					} else {
						if (n != null) {
							if (columnType.isInteger && (!(n instanceof Integer) /*|| n.intValue() < INT_RANGE[0] || n.intValue() > INT_RANGE[1]*/)) {
								columnType.isInteger = false;
							}
							if (columnType.isLong && (!(n instanceof Long) && !(n instanceof Integer) /*|| n.longValue() < BIGINT_RANGE[0] || n.longValue() > BIGINT_RANGE[1]*/)) {
								columnType.isLong = false;
							}
							if (columnType.isDouble && !(n instanceof Double) && !(n instanceof BigDecimal) && !(n instanceof Long) && !(n instanceof Integer)) {
								columnType.isDouble = false;
							}
							columnType.isDate = false;
						} else {
							if (columnType.isDate && d == null) {
								columnType.isDate = false;
							} else {
								columnType.isInteger = false;
								columnType.isLong = false;
								columnType.isDouble = false;
							}
							if (columnType.isBoolean && b == null) {
								columnType.isBoolean = false;
							}
						}
					}
					
					// boolean check
					if (columnType.isBoolean && (b == null || (n != null && n.doubleValue() != 0 && n.doubleValue() != 1) || d != null)) {
						columnType.isBoolean = false;
					}
					
					// set max length
					if (s != null) {
						int l = s.length();
						if (l > columnType.maxLength) {
							columnType.maxLength = l;
						}
					}
					
					// increase value count
					columnType.count++;
				}
			}
			
			// invoke transformers
			/* 2012-03-07 jbornema: deactivated, as in all cases seen so far it should not be executed and the running flag was not used yet 
			for (ITransformer transformer : process.getTransformers()) {
				transformer.postSourceProcessing(context);
			}
			*/
	
			// set collected column type information
			for (Map.Entry<IColumn, ColumnType> entry : columnTypes.entrySet()) {
				IColumn column = entry.getKey();
				ColumnType columnType = entry.getValue();
				
				if (column.getTypeHint() != DataType.UNKNOWN && column.getTypeHint() != DataType.STRING || (column.getTypeHint() == DataType.STRING && column.getLengthHint() > 0 /*2012-10-08 jbornema: disabled to overrule this mode && columnType.count == 0*/)) {
					continue;
				}
				
				boolean forceString = columnType.count == 0; 
				
				if (!forceString) {
					if (columnType.isBoolean) {
						column.setTypeHint(DataType.BOOLEAN);
					} else if (columnType.isInteger) {
						column.setTypeHint(DataType.INT);
					} else if (columnType.isLong) {
						column.setTypeHint(DataType.LONG);
					} else if (columnType.isDouble) {
						column.setTypeHint(DataType.DOUBLE);
					} else if (columnType.isDate) {
						column.setTypeHint(DataType.DATE);
					} else {
						forceString = true;
					}
				}
				
				if (forceString && (column.getTypeHint() == DataType.UNKNOWN || column.getTypeHint() == DataType.STRING)) {
					// TODO support clob for length > 8000
					column.setTypeHint(DataType.STRING);
					// set length
					int lengthHint = 1;
					for (; lengthHint < columnType.maxLength; lengthHint *= 2);
					// set default length
					if (columnType.count == 0) {
						lengthHint = 255;
					}
					if (sqlDialect == SqlDialect.ORACLE) {
						if (lengthHint > 4000 && columnType.maxLength <= 4000) {
							lengthHint = 4000;
						}
					} else {
						if (lengthHint > 8000 && columnType.maxLength <= 8000) {
							lengthHint = 8000;
						}
					}
					column.setLengthHint(lengthHint);
				}
			}
			
			// close source
			extractor.close();
			
			// open again
			ISource source = processContext.getCurrentSource();
			extractor.openSource(source, new DataSet());
		} finally {
			autoDetectColumnTypesRunning = false;
		}
	}

	/**
	 * @param parentName 
	 * @throws SQLException 
	 * 
	 */
	protected void initializeObjectAlias(Object parentName) throws SQLException {
		if (objectAliasByName == null) {
			objectAliasByName = new HashMap<String, String>();
			objectNameByAlias = new HashMap<String, String>();
			// load all aliases
			DatabaseMetaData metaData = connection.getMetaData();
			String tablename = "XWIC_ETL_OBJECT_ALIAS";
			ResultSet rs_tables = metaData.getTables(catalogname == null ? connection.getCatalog() : catalogname, null, tablename, null);
			try {
				if (rs_tables.next()) {
					// load all existing aliases
					Statement stmt = connection.createStatement();
					ResultSet rs = stmt.executeQuery("select PARENT, NAME, ALIAS from " + tablename + " where PARENT " + (parentName == null ? "is null" : "= '" + parentName + "'"));
					try {
						while (rs.next()) {
							String p = rs.getString(1);
							String n = rs.getString(2);
							String a = rs.getString(3);
							objectAliasByName.put(((p != null ? p + "." : ".") + n).toUpperCase(), a);
							objectNameByAlias.put(((p != null ? p + "." : ".") + a).toUpperCase(), n);
						}
					} finally {
						rs.close();
						stmt.close();
					}
				} else {
					// create table
					List<DbColumnDef> dbColumnsDef = new ArrayList<DbColumnDef>();
					dbColumnsDef.add(getDbColumnDef(tablename, "PARENT", DataType.STRING, 128, true));
					dbColumnsDef.add(getDbColumnDef(tablename, "NAME", DataType.STRING, 128, true));
					dbColumnsDef.add(getDbColumnDef(tablename, "ALIAS", DataType.STRING, objectAliasMaxLength, true));
					createTable(tablename, dbColumnsDef, tablename, false);
				}
			} finally {
				rs_tables.close();
			}
		}
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public String getObjectAlias(String parentName, String name) throws SQLException {
		if (!enableObjectAlias) {
			return name;
		}
		
		if (enableGlobalObjectAlias) {
			parentName = null;
		}
		
		String key = ((parentName != null ? parentName + "." : ".") + name).toUpperCase();
		
		String alias = null;
		if (objectAliasByName != null) {
			alias =  objectAliasByName.get(key);
		}
		if (alias != null) {
			return alias;
		}
		if (sqlDialect != SqlDialect.ORACLE) {
			return name;
		}
		if (name.length() <= objectAliasMaxLength) {
			return name;
		}
		
		// automatically generate shorter name and create alias mapping
		String tablename = "XWIC_ETL_OBJECT_ALIAS";

		initializeObjectAlias(parentName);
		alias =  objectAliasByName.get(key);
		if (alias != null) {
			return alias;
		}
		
		// create new alias
		String keyByAlias = null;
		int i = 0;
		while (alias == null) {
			String tag = i == 0 ? "~" : "~" + i + "~";
			int remove = name.length() - objectAliasMaxLength - tag.length();
			String a = null;
			int head = objectAliasMaxLength / 2 - tag.length() / 2;
			if (remove <= 0) {
				head += (remove) / 2;
			}
			a = name.substring(0, head) + tag;
			a += name.substring(name.length() - (objectAliasMaxLength - a.length()));
			keyByAlias = ((parentName != null ? parentName + "." : ".") + a).toUpperCase();
			if (!objectNameByAlias.containsKey(keyByAlias)) {
				alias = a;
			} else {
				i++;
			}
		}
		
		// insert new alias
		Map<String, DbColumnDef> columns = loadColumns(connection.getMetaData(), tablename);
		IColumn colParent = new Column("PARENT");
		IColumn colName = new Column("NAME");
		IColumn colAlias = new Column("ALIAS");	
		columns.get("PARENT").setColumn(colParent);
		columns.get("NAME").setColumn(colName);
		columns.get("ALIAS").setColumn(colAlias);
		
		String pkSequence = null;
		if (sqlDialect == SqlDialect.ORACLE) {
			pkSequence = "SEQ_" + tablename;
		}
		String sql = buildPreparedStatement(tablename, columns.values(), Mode.INSERT, autoIncrementColumn, pkSequence);
		PreparedStatement ps = connection.prepareStatement(sql);
		Record record = new Record(null);
		record.setData(colParent, parentName);
		record.setData(colName, name);
		record.setData(colAlias, alias);		
		try {
			doInsert(processContext, record, columns.values(), ps, autoIncrementColumn);
		} catch (ETLException ee) {
			if (ee.getCause() instanceof SQLException) {
				throw (SQLException)ee.getCause();
			}
			throw new SQLException(ee);
		} finally {
			ps.close();
		}
		objectAliasByName.put(key, alias);
		objectNameByAlias.put(keyByAlias, name);
		return alias;
		
	}
	
	/**
	 * 
	 * @param sql
	 * @param fromDialect
	 * @param createNewAliases
	 * @return
	 * @throws SQLException 
	 */
	public String convertSql(String sql, SqlDialect fromDialect, boolean createNewAliases) throws SQLException {
		initializeObjectAlias(enableGlobalObjectAlias ? null : originalTablename);
		
		if (fromDialect == SqlDialect.MSSQL && sqlDialect == SqlDialect.ORACLE) {
			
			// try to do simple MSSQL to ORACLE conversion
			Pattern sqlQuotePattern = Pattern.compile("([\\[])[^\\[\\]\\n]+([\\]])");
			Matcher matcher = sqlQuotePattern.matcher(sql);
			String s = sql;
			while (matcher.find()) {
				for (int i = 1; i <= matcher.groupCount(); i++) {
					s = s.substring(0, matcher.start(i)) + is + s.substring(matcher.end(i));
				}
			}
			
			// replace all global alias
			Map<String, String> replaceMap = new TreeMap<String, String>(new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					int c = new Integer(s2.length()).compareTo(s1.length());
					if (c != 0) {
						return c;
					}
					return s1.compareTo(s2);
				}
			});
			for (Entry<String, String> e : objectAliasByName.entrySet()) {
				String n = e.getKey();
				String a = e.getValue();
				if (n.startsWith(".")) {
					// use global alias mappings only
					n = n.substring(1);
					replaceMap.put(n, a);
				}
			}
			
			// replace long names first
			for (Entry<String, String> e : replaceMap.entrySet()) {
				String n = e.getKey();
				String a = e.getValue();
				s = s.replaceAll("(?i)\\Q" + n + "\\E", is + a + is);
			}
			
			// replace all double quotes
			s = s.replace(is + is, is);
			
			if (createNewAliases) {
				replaceMap.clear();
				// check if new aliases are required
				Pattern longNamePattern = Pattern.compile("\"([^\"\\n]*)\"");
				matcher = longNamePattern.matcher(s);
				while (matcher.find()) {
					String n = matcher.group(1);
					if (n.length() > objectAliasMaxLength) {
						String a = getObjectAlias(enableGlobalObjectAlias ? null : originalTablename, n);
						replaceMap.put(n, a);
					}
				}

				// replace long names first
				for (Entry<String, String> e : replaceMap.entrySet()) {
					String n = e.getKey();
					String a = e.getValue();
					s = s.replaceAll("(?i)\\Q" + n + "\\E", is + a + is);
				}
				
				// replace all double quotes
				s = s.replace(is + is, is);
				
			}
			
			return s;
		}
		
		// nothing to convert
		return sql;
	}
	
	/**
	 * Create table with primary key column Id (bigint identidy).
	 * @throws SQLException
	 */
	protected void createTable(String tablename, List<DbColumnDef> dbColumnsDef) throws SQLException {
		createTable(tablename, dbColumnsDef, originalTablename, true);
	}
	
	/**
	 * Create table with primary key column Id (bigint identidy).
	 * @throws SQLException
	 */
	protected void createTable(String tablename, List<DbColumnDef> dbColumnsDef, String originalTablename, boolean useObjectAlias) throws SQLException {
		
		String aCol = is + autoIncrementColumn + is;
		boolean defaultInvocation = tablename == this.tablename;
		
		processContext.getMonitor().logInfo("Creating missing table: " + tablename);

		Statement stmt = connection.createStatement();
		try {
			StringBuilder sql = new StringBuilder();
	
			StringBuilder columnsDef = new StringBuilder();
	
			for (DbColumnDef dbcd : dbColumnsDef) {
				if (dbcd.getName().equals(autoIncrementColumn)) {
					// skip default primary key column
					continue;
				}
				columnsDef.append(", " + is + (useObjectAlias ? getObjectAlias(originalTablename, dbcd.getName()) : dbcd.getName()) + is + " " + dbcd.getTypeNameDetails());
			}
			String pkSequence = null;
			if (isSimulatePkIdentity()) {
				sql.setLength(0);
				pkSequence = defaultInvocation && this.pkSequence != null ? this.pkSequence : useObjectAlias ? getObjectAlias(null, "SEQ_" + originalTablename) : "SEQ_" + originalTablename;
				sql.append("CREATE SEQUENCE " + is + pkSequence + is);
				processContext.getMonitor().logInfo("Creating missing sequence for primary key column on table " + getTablenameQuoted(tablename) + ":\n" + sql.toString());
				stmt.execute(sql.toString());
				if (defaultInvocation) {
					this.pkSequence = pkSequence;
				}
			}
			
			sql.setLength(0);
			switch (sqlDialect) {
			case MSSQL :
				sql.append("CREATE TABLE " + getTablenameQuoted(tablename) + " (");
				sql.append(aCol + " BIGINT IDENTITY(1,1) NOT NULL" + columnsDef + ", CONSTRAINT " + is + "PK_" + tablename + is + " PRIMARY KEY (" + aCol + "))");
				break;
			case H2 : 
				sql.append("CREATE TABLE " + getTablenameQuoted(tablename) + " (");
				sql.append(aCol + " BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY" + columnsDef + ")");
				break;
			case ORACLE :
				String seq = "";
				if (pkSequence != null) {
					seq = " DEFAULT " + is + pkSequence + is + ".NEXTVAL";
				}
				sql.append("CREATE TABLE " + getTablenameQuoted(tablename) + " (");
				sql.append(aCol + " NUMBER(20)" + seq + " NOT NULL" + columnsDef + ", CONSTRAINT " + is + (useObjectAlias ? getObjectAlias(null, "PK_" + originalTablename) : "PK_" + originalTablename) + is + " PRIMARY KEY (" + aCol + "))");
				break;
			case MYSQL :
				sql.append("CREATE TABLE " + getTablenameQuoted(tablename) + " (");
				sql.append(aCol + " BIGINT NOT NULL AUTO_INCREMENT" + columnsDef + " , CONSTRAINT " + is + "PK_" + tablename + is + " PRIMARY KEY (" + aCol + "))");
				break;
			}
			
			processContext.getMonitor().logInfo("Creating missing table sql: \n" + sql.toString());
			
			stmt.execute(sql.toString());
		} finally {
			stmt.close();
		}
	}

	/**
	 * Truncate table
	 * @throws ETLException 
	 */
	protected void truncateTable() throws ETLException {
		try {
			tablePurged = true;
			Statement stmt = connection.createStatement();
			int rows;
			try {
				// try TRUNCATE TABLE
				ResultSet rs = JDBCUtil.executeQuery(stmt, "SELECT COUNT(*) FROM " + getTablenameQuoted());
				try {
					rs.next();
					rows = rs.getInt(1);
				} finally {
					rs.close();
				}
				stmt.executeUpdate("TRUNCATE TABLE " + getTablenameQuoted());
			} catch (SQLException e) {
				// try DELETE FROM
				rows = stmt.executeUpdate("DELETE FROM " + getTablenameQuoted());
			} finally {
				stmt.close();
			}
			processContext.getMonitor().logInfo("TRUNCATED TABLE " + getTablenameQuoted() + " - " + rows + " rows have been deleted.");
			deleteCount += rows;
		} catch (SQLException e) {
			throw new ETLException("Error truncating table: " + e, e);
		}
	}

	/**
	 * Truncate table
	 * @throws ETLException 
	 */
	protected void deleteTable() throws ETLException {
		try {
			tablePurged = true;
			Statement stmt = connection.createStatement();
			try {
				// try DELETE FROM
				int rows = JDBCUtil.executeUpdate(stmt, "DELETE FROM " + getTablenameQuoted());
				processContext.getMonitor().logInfo("DELETE FROM TABLE " + getTablenameQuoted() + " - " + rows + " rows have been deleted.");
				deleteCount += rows;
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new ETLException("Error deleting from table: " + e, e);
		}
	}

	/**
	 * 
	 * @param tablename
	 * @param columnName
	 * @param typeHint
	 * @param lengthHint
	 * @return
	 * @throws SQLException 
	 */
	protected DbColumnDef getDbColumnDef(String tablename, String columnName, DataType typeHint, int lengthHint, boolean allowsNull) throws SQLException {
		IColumn col = new Column(columnName);
		col.setTypeHint(typeHint);
		col.setLengthHint(lengthHint);
		DbColumnDef colDef = getDbColumnDef(tablename, col);
		colDef.setAllowsNull(allowsNull);
		return colDef;
	}
	
	/**
	 * 
	 * @param col
	 * @return
	 * @throws SQLException 
	 */
	protected DbColumnDef getDbColumnDef(String tablename, IColumn col) throws SQLException {

		String DATETIME = "DATETIME";
		String FLOAT = "FLOAT";
		String INT = "INT";
		String BIGINT = "BIGINT";
		String BIT = "BIT";
		String TEXT = "TEXT";
		String VARCHAR = "VARCHAR";
		String BINARY = "BINARY";
		int MAX_VARCHAR = 8000;
		
		if (sqlDialect == SqlDialect.ORACLE) {
			// set Oracle types
			DATETIME = "TIMESTAMP";
			FLOAT = "NUMBER(38,15)";
			INT = "NUMBER(10)";
			BIGINT = "NUMBER(20)";
			BIT = "NUMBER(1)";
			TEXT = "CLOB";
			VARCHAR = "VARCHAR2";
			MAX_VARCHAR = 4000;
		}

		String columnName = col.computeTargetName();
		columnName = getObjectAlias(tablename, columnName);
		
		DbColumnDef dbcd = new DbColumnDef(columnName);
		dbcd.setColumn(col);
		String typeName;
		
		switch (col.getTypeHint()) {
		case DATE:
		case DATETIME:
			typeName = DATETIME;
			dbcd.setType(Types.TIMESTAMP);
			break;
		case DOUBLE:
			typeName = FLOAT;
			dbcd.setType(Types.FLOAT);
			break;
		case INT:
			typeName = INT;
			dbcd.setType(Types.INTEGER);
			break;
		case LONG:
			typeName = BIGINT;
			dbcd.setType(Types.BIGINT);
			break;
        case BIGDECIMAL:
            typeName = BIGINT;
            dbcd.setType(Types.BIGINT);
            break;    
		case BOOLEAN:
			typeName = BIT;
			dbcd.setType(Types.BIT);
			break;
		case BINARY:
			typeName = BINARY;
			dbcd.setType(Types.BINARY);
			dbcd.setSize(col.getLengthHint() > 0 ? col.getLengthHint() : 50);
			break;
		default:
			int length = col.getLengthHint();
			if (length < 1) {
				if (autoAlterColumns) {
					// start with size 1
					length = 1;
				} else {
					length = 255;
				}
				//length = 255;
			}
			if (length > MAX_VARCHAR) { // TODO Please check if varchar(MAX) would be also an option
				typeName = TEXT;
				dbcd.setType(Types.CLOB);
				dbcd.setSize(length);
			} else {
				typeName = VARCHAR;
				dbcd.setType(Types.VARCHAR);
				dbcd.setSize(length);
			}
			break;
		}
		
		dbcd.setTypeName(typeName);
		
		// by default when preventNotNullError is true, columns won't be nullable
		dbcd.setAllowsNull(!preventNotNullError || autoDetectColumnTypesNullable);
		return dbcd;
	}

	/**
	 * @param missingCols
	 * @param columns 
	 * @throws SQLException 
	 */
	private void createColumns(List<IColumn> missingCols, Map<String, DbColumnDef> columns) throws SQLException {

		StringBuilder sqlAlter = new StringBuilder();
		sqlAlter.append("ALTER TABLE ");
		sqlAlter.append(getTablenameQuoted());
		sqlAlter.append(" ADD ");
		
		for (IColumn col : missingCols) {
			
			// if a column is used twice, we must make sure that it is not created twice as well!
			if (!columns.containsKey(col.computeTargetName())) {
				String originalColumnName = col.computeTargetName();
				DbColumnDef dbcd = getDbColumnDef(tablename, col);
				
				if (!originalColumnName.equals(dbcd.getName())) {
					monitor.logWarn(this + " uses column alias " + dbcd.getName() + " for originally configured column " + originalColumnName);
				}
				
				StringBuilder sql = new StringBuilder(sqlAlter);
				sql.append(is + dbcd.getName() + is + " " + dbcd.getTypeNameDetails());
				/* 2013-08-23 jbornema: Not yet tested 
				if (!dbcd.isAllowsNull()) {
					String defaultValue = getColumnDefaultValueForNull(dbcd);
					sql.append(" CONSTRAINT " + is + "DF_" + tablename + "_" + dbcd.getName() + is + " DEFAULT '" + defaultValue + "'");	
				}
				*/
				
				processContext.getMonitor().logInfo("Creating missing column: \n" + sql.toString());

				Statement stmt = connection.createStatement();
				try {
					JDBCUtil.executeUpdate(stmt, sql.toString());
				} finally {
					stmt.close();
				}
				
				dbcd.setColumn(col);
				columns.put(dbcd.getName(), dbcd);
			}
			
		}
	}

	/**
	 * 
	 * @return
	 */
	protected String getTablenameQuoted() {
		return getTablenameQuoted(tablename);
	}
	
	/**
	 * 
	 * @param tablename
	 * @return
	 */
	protected String getTablenameQuoted(String tablename) {
		StringBuilder sql = new StringBuilder();
		// TODO missing support for other dialects like ORACLE...
		if (sqlDialect == SqlDialect.MSSQL) {
			if (catalogname != null) {
				sql.append(is + catalogname + is + "." + is + schemaname != null ? schemaname : "" + is + ".");
			} else if (schemaname != null) {
				sql.append(is + schemaname + is + ".");
			}
		}
		sql.append(is + tablename + is);
		return sql.toString();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext context, IRecord record) throws ETLException {
		
		processRecord(context, record, true);
		
	}
	
	protected void processRecord(IProcessContext context, IRecord record, boolean retry) throws ETLException {
		try {
			long start = System.nanoTime();
			switch (mode) {
			case INSERT:
				doInsert(context, record, columns.values(), psInsert, pkColumn != null ? pkColumn : autoIncrementColumn);
				break;
			case UPDATE:
				doUpdate(context, record);
				break;
			case INSERT_OR_UPDATE:
				if (Validate.equals(newIdentifierValue, record.getData(newIdentifierColumn))) {
					doInsert(context, record, columns.values(), psInsert, pkColumn);
				} else {
					doUpdate(context, record);
				}
			}
			processRecordCount += 1;
			nanoTimeProcessRecord += (System.nanoTime() - start);
		} catch (Throwable t) {
			boolean skipFinally = false;
			boolean closed = false;
			try {
				if (retry && reopenClosedConnection) {
					closed = true;
					try {
						if (connection != null) {
							closed = connection.isClosed();
						}
					} catch (SQLException e) {}
					if (closed) {
						if (connection != null) {
							monitor.logError("Connection closed, try to reopen in 1 Minute...", t);
							try {
								Thread.sleep(60000);
							} catch (InterruptedException e) {
								throw new ETLException(e);
							}
						}
						// try to open connection again
						initConnection(context);
						try {
							if (columns == null) {
								checkTableStructure(true);
							}
							buildPreparedStatements(true);
						} catch (SQLException e) {
							monitor.logError("Cannot build prepared statement ", e);
						}
						processRecord(context, record, false);
						skipFinally = true;
					}
				}
			} finally {
				if (!skipFinally) {
					if (closed) {
						// failed to reopen connection
						monitor.logError("Failed to reopen connection, disable reopen closed connection setting");
						setReopenClosedConnection(false);
					}
					record.markInvalid(t.getLocalizedMessage());
					String msg = "Cannot process record " + context.getRecordsCount();
					if (skipError) {
						context.getMonitor().logError(msg , t);
					} else {
						throw new ETLException(msg, t);
					}
				}
			}
		}
	}

	/**
	 * Invoked after update statement received changed record data to allow customer flagging.
	 * Must return true to update the prepared statement again. 
	 * @param context
	 * @param record
	 * @return
	 * @throws ETLException
	 */
	protected boolean onRecordUpdated(IProcessContext context, IRecord record) throws ETLException {
		return false;
	}
	
	private void doUpdate(IProcessContext context, IRecord record) throws ETLException {
		boolean handleException = true;
		try {
			psUpdate.clearParameters();
			
			int idx = 1;
			boolean modified = false;
			DbColumnDef pkColDef = null;
			
			for (int i = 0; i < 2; i++) { 
				for (DbColumnDef colDef : columns.values()) {
					// Identity is not support on ORACLE
					if (colDef.getColumn() != null && !colDef.getName().equalsIgnoreCase(pkColumn) && !colDef.isReadOnly()) {
						
						Object value = record.getData(colDef.getColumn());
						setPSValue(psUpdate, idx++, value, colDef);
						
						modified = modified | record.isChanged(colDef.getColumn());
						
					}
	
					// PK might be excluded
					if (colDef.getName().equals(pkColumn)) {
						pkColDef = colDef;
					}
				}
				
				if (modified && onRecordUpdated(context, record)) {
					// record updated and onRecordUpdated return true to run again
					idx = 1;
				} else {
					// early exist
					break;
				}
			}

			if (pkColDef == null) {
				throw new ETLException("The specified PK Column does not exist.");
			}
			setPSValue(psUpdate, idx, record.getData(pkColumn), pkColDef);
			
			if (!ignoreUnchangedRecords || modified) {

				if (batchSize < 1) {
					// non-batched mode
					int count = psUpdate.executeUpdate();
					if (count != 1) {
						monitor.logWarn("Update resulted in count " + count + " but expected 1");
					}	
					updateCount += count;
				} else {
					// batched mode
					psUpdate.addBatch();
					batchUpdateRecords.add(record);
					batchCountUpdate++;
					if (batchCountUpdate >= batchSize) {
						// execute
						handleException = false;
						executeBatch();
					}
				}
			}				
		} catch (Throwable t) {
			if (handleException) {
				handleException(t, record, context.getRecordsCount(), true);
			} else {
				throw new ETLException(t);
			}
		}
		
	}

	
	/**
	 * @param psUpdate2 
	 * @param i
	 * @param value
	 * @param colDef 
	 * @throws SQLException 
	 * @throws ETLException 
	 */
	protected void setPSValue(PreparedStatement ps, int idx, Object value, DbColumnDef colDef) throws SQLException, ETLException {
		boolean emptyAsNull = treatEmptyAsNull;
		if (preventNotNullError && value == null && !colDef.isAllowsNull()) {
			// get value for null
			value = getColumnDefaultValueForNull(colDef);
			emptyAsNull = false;
		}
		
		if (value == null || (emptyAsNull && value instanceof String && ((String)value).length() == 0)) {
			ps.setNull(idx, colDef.getType());
		} else {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR: {
				String s = value.toString();
				if (s.length() > colDef.getSize()) {
					if (autoDataTruncate) {
						monitor.logWarn("Truncate value for column '" + colDef.getName() + "' from " + s.length() + " to " + colDef.getSize() + " character");
						s = s.substring(0, colDef.getSize());
					} else if (autoAlterColumns) {
						// data truncate sql exception will happen: alter pro-active the table
						// empty batch, TODO shared or open connections might be checked that could cause a looked situation
						executeBatch();
						alterColumnSize(colDef, s.length());
					}
				}
				ps.setString(idx, s);
				break;
			}
			case Types.CLOB:
			case Types.NCLOB: {
				String s = value.toString();
				ps.setString(idx, s);
				break;
			}
			case Types.BIGINT:
				try {
					if (value instanceof Number) {
						ps.setLong(idx, ((Number)value).longValue());
					} else if (value instanceof String) {
						ps.setLong(idx, Long.parseLong((String)value));
					} else {
						// unknown value
						ps.setObject(idx, value);
					}
					break;
				} catch (NumberFormatException nfe) {
					if (autoAlterColumns) {
						// check if column can be converted to float
						try {
							float f = Float.parseFloat(value.toString());
							// data truncate sql exception might happen: alter pro-active the table
							// empty batch, TODO shared or open connections might be checked that could cause a looked situation
							executeBatch();
							alterColumnType(colDef, Types.FLOAT, null);
							ps.setFloat(idx, f);
							break;
						} catch (NumberFormatException e) {}
						if (autoAlterColumnsMode == AutoAlterColumnsMode.ANY_TO_VARCHAR) {
							executeBatch();
							alterColumnType(colDef, Types.VARCHAR, value);
							ps.setString(idx, value.toString());
							break;
						}
					}
					// add column information
					throw new ETLException(nfe.getMessage() + ", column '" + colDef.getName() + "'", nfe);
				}
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
				try {
					if (value instanceof Integer) {
						ps.setInt(idx, (Integer)value);
					} else if (value instanceof Long) {
						if (autoAlterColumns) {
							// data truncate sql exception might happen: alter pro-active the table
							// empty batch, TODO shared or open connections might be checked that could cause a looked situation
							executeBatch();
							alterColumnType(colDef, Types.BIGINT, null);
						}
						ps.setLong(idx, (Long)value);
					} else if (value instanceof String) {
						ps.setInt(idx, Integer.parseInt((String)value));
					} else if (value instanceof Number) {
						ps.setInt(idx, ((Number)value).intValue());
					} else {
						// unknown value
						ps.setObject(idx, value);
					}
					break;
				} catch (NumberFormatException nfe) {
					if (autoAlterColumns) {
						// check if column can be converted to long or float
						try {
							long l = Long.parseLong(value.toString());
							// data truncate sql exception might happen: alter pro-active the table
							// empty batch, TODO shared or open connections might be checked that could cause a looked situation
							executeBatch();
							alterColumnType(colDef, Types.BIGINT, null);
							ps.setLong(idx, l);
							break;
						} catch (NumberFormatException e) {}
						try {
							float f = Float.parseFloat(value.toString());
							// data truncate sql exception might happen: alter pro-active the table
							// empty batch, TODO shared or open connections might be checked that could cause a looked situation
							executeBatch();
							alterColumnType(colDef, Types.FLOAT, null);
							ps.setFloat(idx, f);
							break;
						} catch (NumberFormatException e) {}
						if (autoAlterColumnsMode == AutoAlterColumnsMode.ANY_TO_VARCHAR) {
							executeBatch();
							alterColumnType(colDef, Types.VARCHAR, value);
							ps.setString(idx, value.toString());
							break;
						}
					}
					// add column information
					throw new ETLException(nfe.getMessage() + ", column '" + colDef.getName() + "'", nfe);
				}
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
			case Types.NUMERIC:
				try {
					if (value instanceof Integer) {
						ps.setInt(idx, (Integer)value);
					} else if (value instanceof Long) {
						ps.setLong(idx, (Long)value);
					} else if (value instanceof String) {
						String s = (String)value;
						if (s.length() == 0) {
							ps.setNull(idx, colDef.getType());
						} else {
							ps.setDouble(idx, Double.parseDouble(s));
		                }
					} else if (value instanceof Double) {
						ps.setDouble(idx, (Double)value);
	                } else if (value instanceof BigDecimal) {
	                    ps.setBigDecimal(idx, (BigDecimal)value);
					} else {
						// unknown value
						ps.setObject(idx, value);
					}
				} catch (NumberFormatException nfe) {
					if (autoAlterColumns) {
						if (autoAlterColumnsMode == AutoAlterColumnsMode.ANY_TO_VARCHAR) {
							executeBatch();
							alterColumnType(colDef, Types.VARCHAR, value);
							ps.setString(idx, value.toString());
							break;
						}
					}
					// add column information
					throw new ETLException(nfe.getMessage() + ", column '" + colDef.getName() + "'", nfe);
				}
				break;
			case Types.TIMESTAMP:
			case Types.DATE:
			case Types.TIME:
				if (value instanceof Date) {
					//ps.setDate(idx, new java.sql.Date(((Date)value).getTime())); // this loses the time information
					ps.setTimestamp(idx, new java.sql.Timestamp(((Date)value).getTime()));
				} else if (value instanceof java.sql.Timestamp) {
					ps.setTimestamp(idx, (java.sql.Timestamp)value);
				} else if (value instanceof java.sql.Date) {
					ps.setDate(idx, (java.sql.Date)value);
				} else if (value instanceof java.sql.Time) {
					ps.setTime(idx, (java.sql.Time)value);
				} else if (value instanceof String) {
					String s = (String)value;
					if (s.length() == 0) {
						ps.setNull(idx, colDef.getType());
					} else {
						ps.setString(idx, s);
					}
				} else {
					// unknown value
					ps.setObject(idx, value);
				}
				break;
			case Types.BIT:
			case Types.BOOLEAN:
				if (value instanceof Boolean) {
					Boolean b = (Boolean)value;
					ps.setBoolean(idx, b.booleanValue());
				} else if (value instanceof Number) {
					Number valNum = (Number) value;
					ps.setBoolean(idx, valNum.intValue() == 0 ? false : true);
				} else if (value instanceof String) {
					try {
						Integer valInt = Integer.parseInt((String) value);
						ps.setBoolean(idx, valInt == 0 ? false : true);
					} catch (NumberFormatException nfe) {
						// unknown value
						ps.setObject(idx, value);
					}
				} else {
					// unknown value
					ps.setObject(idx, value);
				}
				break;
			case Types.OTHER:
			case Types.JAVA_OBJECT:
			case Types.BINARY:
			case Types.BLOB:
			case Types.LONGVARBINARY:
				ps.setObject(idx, value);
				break;
			default:
				throw new ETLException("Unknown datatype: "+ colDef.getType());
			}
			
		}
		
	}

	/**
	 * Alter column to specified size.
	 * @param colDef
	 * @param length
	 * @throws SQLException 
	 */
	protected void alterColumnSize(DbColumnDef colDef, int size) throws SQLException {
		synchronized (JDBCUtil.DBCOLUMNDEF_ALTERED) {
			
			String globalKey = JDBCUtil.makeGlobalDbColumnDefKey(connectionName, schemaname, tablename, colDef);
			DbColumnDef alteredDbColDef = JDBCUtil.DBCOLUMNDEF_ALTERED.get(globalKey);

			// set length
			int newSize = 1;
			for (; newSize < size; newSize *= 2);
			
			if (alteredDbColDef != null) {
				if (alteredDbColDef.getSize() > newSize) {
					newSize = alteredDbColDef.getSize();
				}
			}
			
			String typeName = colDef.getTypeName();
			if (typeName == null) {
				// identify column type
				throw new SQLException("Missing column '" + colDef.getName() + "' typeName");
			}
	
			String nullable = colDef.isAllowsNull() ? "NULL" : "NOT NULL";
	
			// alter to new type
			String newType = typeName + "(" + newSize + ")";
			
			StringBuilder sb = new StringBuilder();
	
			switch (sqlDialect) {
			case ORACLE: {
				if (newSize > 4000) {
					switch (colDef.getType()) {
					case Types.VARCHAR:
					case Types.NVARCHAR:
						if (colDef.getSize() < 4000) {
							processContext.getMonitor().logWarn("Max size for varchar is 4000, reduced " + newSize + " to 4000");
							newType = typeName + "(4000)";
							newSize = 4000;
						} else {
							processContext.getMonitor().logError("Max size for varchar is 4000, cannot apply new size " + newSize);
							return;
						}
						break;
					}
				}
				sb.append("ALTER TABLE " + getTablenameQuoted() + " MODIFY " + is);
				sb.append(colDef.getName() + is + " " + newType);
				//.append(" " + nullable); causes ORA-01451: column to be modified to NULL cannot be modified to NULL
				break;
			}
			case MYSQL: {
				if (newSize > 65535) {
					switch (colDef.getType()) {
					case Types.VARCHAR:
					case Types.NVARCHAR:
						if (colDef.getSize() < 65535) {
							processContext.getMonitor().logWarn("Max size for varchar is 65535, reduced " + newSize + " to 65535");
							newType = typeName + "(65535)";
							newSize = 65535;
						} else {
							processContext.getMonitor().logError("Max size for varchar is 65535, cannot apply new size " + newSize);
							return;
						}
						break;
					}
				}
				sb.append("ALTER TABLE " + getTablenameQuoted() + " MODIFY " + is);
				sb.append(colDef.getName() + is + " " + newType);
				//.append(" " + nullable); causes ORA-01451: column to be modified to NULL cannot be modified to NULL
				break;
			}
			default: /* Tested with MSSQL */ {
				// check if new type is valid: TODO Retrieve that info from jdbc meta data or so...
				if (newSize > 8000) {
					switch (colDef.getType()) {
					case Types.VARCHAR:
					case Types.NVARCHAR:
						// convert to MAX
						newType = typeName + "(MAX)";
						break;
					}
				}
				sb.append("ALTER TABLE " + getTablenameQuoted() + " ALTER COLUMN " + is);
				sb.append(colDef.getName() + is + " " + newType + " " + nullable);
				break;
			}
			}
			
			processContext.getMonitor().logWarn("Alter column " + is + colDef.getName() + is + " size from " + colDef.getSize() + " to " + newSize);
	
			processContext.getMonitor().logInfo(sb.toString());
			
			// alter column
			Statement stmt = connection.createStatement();
			try {
				JDBCUtil.executeUpdate(stmt, sb.toString());
			} finally {
				stmt.close();
			}
			
			colDef.setSize(newSize);
			if (alteredDbColDef != null) {
				alteredDbColDef.setSize(newSize);
			} else {
				 JDBCUtil.DBCOLUMNDEF_ALTERED.put(globalKey, colDef);
			}
		}		
	}

	/**
	 * Alter column to specified type (if supported by db).
	 * @param colDef
	 * @param type
	 * @param value
	 * @throws SQLException
	 */
	protected void alterColumnType(DbColumnDef colDef, int type, Object value) throws SQLException {
		int size = colDef.getSize();
		int scale = colDef.getScale();
		String newTypeName = null;
		String newTypeDef = null;
		String newTypeSize = "";
		switch (type) {
		case Types.BIGINT:
			if (sqlDialect == SqlDialect.ORACLE) {
				newTypeName = "NUMBER(20)";
			} else {
				newTypeName = "bigint";
			}
			break;
		case Types.FLOAT:
			if (sqlDialect == SqlDialect.ORACLE) {
				newTypeName = "NUMBER(38,15)";
			} else {
				newTypeName = "float";
			}
			break;
		case Types.VARCHAR:
			// TODO support clob for length > 8000
			// set length
			size = 1;
			for (; size < value.toString().length(); size *= 2);
			// set default length
			if (size < 1) {
				size = 1;
			}
			int maxVarchar = 8000;
			if (sqlDialect == SqlDialect.ORACLE) {
				maxVarchar = 4000;
			}
			if (size > maxVarchar && value.toString().length() <= maxVarchar) {
				size = maxVarchar;
			}
			if (size > maxVarchar) {
				throw new SQLException("Cannot alter column " + colDef + " to varchar, value to large: " + value);
			}
			colDef.setSize(size);
			colDef.setScale(0);
			if (sqlDialect == SqlDialect.ORACLE) {
				newTypeName = "VARCHAR2";
			} else {
				newTypeName = "VARCHAR";
			}
			newTypeSize = "(" + size + ")";
			break;
		default:
			throw new SQLException("Unsupported column type " + type + " for alter column " + colDef);
		}
		String nullable = colDef.isAllowsNull() ? " NULL" : " NOT NULL";
		if (sqlDialect == SqlDialect.ORACLE) {
			// not supported
			nullable = "";
		}
		newTypeDef = newTypeName + newTypeSize;
		processContext.getMonitor().logWarn("Alter table " + getTablenameQuoted() + " column " + colDef + " to " + newTypeDef + nullable);

		String typeName = colDef.getTypeName();
		if (typeName == null) {
			// identify column type
			throw new SQLException("Missing column '" + colDef.getName() + "' typeName");
		}
		
		// alter column
		Statement stmt = connection.createStatement();
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("ALTER TABLE " + getTablenameQuoted());
			if (sqlDialect == SqlDialect.ORACLE) {
				sb.append(" MODIFY ");
			} else {
				sb.append(" ALTER COLUMN ");
			}
			sb.append(is + colDef.getName() + is + " " + newTypeDef + nullable);
			
			processContext.getMonitor().logInfo(sb.toString());
			
			JDBCUtil.executeUpdate(stmt, sb.toString());
		} finally {
			stmt.close();
		}
		
		colDef.setType(type);
		colDef.setTypeName(newTypeName);
		colDef.setSize(size);
		colDef.setScale(scale);
		
	}

	
	private void doInsert(IProcessContext context, IRecord record, Collection<DbColumnDef> columns, PreparedStatement ps, String pkColumn) throws ETLException {
		boolean handleException = true;
		boolean defaultInvocation = ps == psInsert; // if defaultInvocation is false, then batch insert is not used (only used for the "Oracle" dictionary "workaround")
		try {
			ps.clearParameters();
			
			int idx = 1;
			for (DbColumnDef colDef : columns) {
				if (colDef.getColumn() != null || (/* support preventNotNullError */ preventNotNullError && !colDef.isAllowsNull())) {
					
					// ignore identity columns for insert and update (identity is not supported on ORACLE)
					if (colDef.isReadOnly() || (!isSimulatePkIdentity() && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn))) {
						continue;
					}
					
					Object value = colDef.getColumn() != null ? record.getData(colDef.getColumn()) : null;
					setPSValue(ps, idx++, value, colDef);
					
				}
			}
			
			if (batchSize < 1 || !defaultInvocation) {
				// non-batched mode
				int count = ps.executeUpdate();
				if (count == -2) {
					/* For some unknown reason (Oracle, version: Oracle9i Enterprise Edition Release 9.2.0.4.0) 
					 * using Oracle Instant Client 11.2.0.1.0 returns -2 */
					count = 1;
				}
				if (count != 1 && defaultInvocation) {
					monitor.logWarn("Insert resulted in count " + count + " but expected 1");
				}
				if (defaultInvocation) {
					insertCount += count;
				}
			} else {
				// batched mode
				ps.addBatch();
				batchInsertRecords.add(record);
				batchCountInsert++;
				if (batchCountInsert >= batchSize) {
					// execute
					handleException = false;
					executeBatch();
				}
			}
			
		} catch (Throwable t) {
			if (handleException) {
				handleException(t, record, context.getRecordsCount(), true);
			} else {
				throw new ETLException(t);
			}
		}
		
	}

	/**
	 * 
	 * @param se
	 * @param record
	 * @throws ETLException 
	 */
	protected void handleException(Throwable t, IRecord record, Integer recordsCount, boolean throwException) throws ETLException {
		if (record != null && (t instanceof DataTruncation || t.getCause() instanceof DataTruncation || (t.getMessage() != null && t.getMessage().toLowerCase().contains("truncation")))) {
			//monitor.logError("Data Truncation during INSERT record (fields with value lengths following): " + record, se);
			// log field value lengths
			DbColumnDef hintColDef = null;
			int valueSize = 0;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					Object value = record.getData(colDef.getColumn());
					if (value instanceof Long && colDef.getType() != Types.BIGINT) {
						hintColDef = colDef;
						monitor.logError("Column '" + colDef.getName() + "' of type " + colDef.getTypeName() + " requires BIGINT on record " + record);
					}
					if (!(value instanceof String)) {
						continue;
					}
					switch (colDef.getType()) {
					case Types.INTEGER:
						continue;
					}
					valueSize = value.toString().length();
					if (colDef.getSize() < valueSize && hintColDef == null /*&& colDef.getColumn().getTypeHint() == DataType.STRING*/) {
						hintColDef = colDef;
						monitor.logError("Column '" + colDef.getName() + "' of size " + colDef.getSize() + " requires " + valueSize + " on record " + record);
					}
					//monitor.logInfo(colDef.getName() + "{" + colDef.getSize() + "}=" + value + "{" + value.toString().length() + "}");
				}
			}
			
			if (hintColDef == null && !throwException) {
				monitor.logError("A Data Truncation occurred during INSERT or UPDATE on record " + recordsCount + ": " + record);
			}
			if (throwException) {
				throw new ETLException("A Data Truncation occurred during INSERT or UPDATE on record " + recordsCount + ": " + record, t);
			}
		} else {
			String msg = record != null ? "A SQLException occurred during INSERT or UPDATE on record " + recordsCount + ": " + record
										: "A SQLException occurred during INSERT or UPDATE";
			if (throwException) {
				throw new ETLException(msg, t);
			} else {
				monitor.logError(msg, t);
			}
		}
	}

	/**
	 * @throws SQLException 
	 * @throws ETLException 
	 * 
	 */
	public void executeBatch() throws ETLException {
		// batch insert
		try {
			int[] result = null;
			List<IRecord> batchRecords = null;
			try {
				if (batchCountInsert > 0) {
					long start = System.nanoTime();
					batchRecords = batchInsertRecords;
					result = psInsert.executeBatch();
					for (int i = 0; i < result.length; i++) {
						int count = result[i];
						if (count == -2) {
							/* For some unknown reason (Oracle, version: Oracle9i Enterprise Edition Release 9.2.0.4.0) 
							 * using Oracle Instant Client 11.2.0.1.0 returns -2 */
							count = 1;
						}
						if (count != 1) {
							monitor.logWarn("Insert resulted in count " + count + " but expected 1");
						}
						if (count > 0) {
							insertCount += count;
						}
					}
					batchCountInsert = 0;
					nanoTimeExecuteBatch += (System.nanoTime() - start);
				}
				
				// batch update
				if (batchCountUpdate > 0) {
					long start = System.nanoTime();
					batchRecords = batchUpdateRecords;
					result = psUpdate.executeBatch();
					for (int i = 0; i < result.length; i++) {
						int count = result[i];
						if (count != 1) {
							monitor.logWarn("Update resulted in count " + count + " but expected 1");
						}	
						updateCount += count;
					}			
					batchCountUpdate = 0;
					nanoTimeExecuteBatch += (System.nanoTime() - start);
				}
			} catch (BatchUpdateException bue) {
				// find record
				if (result == null) {
					result = bue.getUpdateCounts();
				}
				if (result != null && batchRecords != null) {
					IRecord record = null;
					for (int i = 0; i < result.length; i++) {
						if (result[i] != 1) {
							record = batchRecords.get(i);
							handleException(bue, record, batchRecordsCountOffset + i, false);
						}
					}
				}
				handleException(bue, null, null, true);
			} catch (SQLException se) {
				handleException(se, null, null, true);
			}
		} finally {
			batchInsertRecords.clear();
			batchUpdateRecords.clear();
			batchRecordsCountOffset = processContext.getRecordsCount();
		}
	}

	/**
	 * @return the driverName
	 */
	public String getDriverName() {
		return driverName;
	}

	/**
	 * @param driverName the driverName to set
	 */
	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}

	/**
	 * @return the connectionUrl
	 */
	public String getConnectionUrl() {
		return connectionUrl;
	}

	/**
	 * @param connectionUrl the connectionUrl to set
	 */
	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the tablename
	 * @deprecated
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * @return the tablename
	 */
	public String getTableName() {
		return tablename;
	}

	/**
	 * @param tablename the tablename to set
	 * @deprecated
	 */
	public void setTablename(String tablename) {
		setTableName(tablename);
	}

	/**
	 * @param tablename the tablename to set
	 */
	public void setTableName(String tablename) {
		if (tablename != null && sqlDialect == SqlDialect.ORACLE) {
			// upper case table name
			//tablename = tablename.toUpperCase();
		}
		if (schemaname == null) {
	        //RPF: identifying, if the tablename contains a "." to separate schema and table name!
	        String schema = null;
	        String rawTableName = tablename;
	
	        if (tablename.contains(".")) {
	            schema = tablename.substring(0, tablename.indexOf("."));
	            rawTableName = tablename.substring(tablename.indexOf(".") + 1, tablename.length());
	        }
	
	        this.tablename = rawTableName;
	        setSchemaName(schema);
		}		
		this.tablename = tablename;
	}

	/**
	 * @param catalogname the catalogname to set
	 */
	public void setCatalogName(String catalogname) {
		this.catalogname = catalogname;
	}

	/**
	 * @return the catalogname
	 */
	public String getCatalogName() {
		return catalogname;
	}

	/**
	 * @return the autoCreateColumns
	 */
	public boolean isAutoCreateColumns() {
		return autoCreateColumns;
	}

	/**
	 * @param autoCreateColumns the autoCreateColumns to set
	 */
	public void setAutoCreateColumns(boolean autoCreateColumns) {
		this.autoCreateColumns = autoCreateColumns;
	}

	/**
	 * Add columns that exist in the target table but are not touched. This eliminates
	 * a warning about columns that exist in the table but do not come from the source.
	 * @param columns
	 */
	public void addIgnoreableColumns(String... columns) {
		
		for (String s : columns) {
			ignoredColumns.add(s);
		}
		
	}

	/**
	 * Exclude columns from being loaded.
	 * @param columns
	 */
	public void addExcludedColumns(String... columns) {
		
		for (String s : columns) {
			excludedColumns.add(s);
		}
		
	}
	
	/**
	 * @return the ignoreMissingTargetColumns
	 */
	public boolean isIgnoreMissingTargetColumns() {
		return ignoreMissingTargetColumns;
	}

	/**
	 * @param ignoreMissingTargetColumns the ignoreMissingTargetColumns to set
	 */
	public void setIgnoreMissingTargetColumns(boolean ignoreMissingTargetColumns) {
		this.ignoreMissingTargetColumns = ignoreMissingTargetColumns;
	}

	/**
	 * @return the connectionName
	 */
	public String getConnectionName() {
		return connectionName;
	}

	/**
	 * @param connectionName the connectionName to set
	 */
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}
	
	/**
	 * @return the truncateTable
	 */
	public boolean isTruncateTable() {
		return truncateTable;
	}

	/**
	 * @param truncateTable the truncateTable to set
	 */
	public void setTruncateTable(boolean truncateTable) {
		this.truncateTable = truncateTable;
	}
	
	/**
	 * @return the autoDataTruncate
	 */
	public boolean isAutoDataTruncate() {
		return autoDataTruncate;
	}
	
	/**
	 * @param autoDataTruncate the autoDataTruncate to set
	 */
	public void setAutoDataTruncate(boolean autoDataTruncate) {
		this.autoDataTruncate = autoDataTruncate;
	}

	/**
	 * @return the mode
	 */
	public Mode getMode() {
		return mode;
	}

	/**
	 * @param mode the mode to set
	 */
	public void setMode(Mode mode) {
		this.mode = mode;
	}

	/**
	 * @return the pkColumn
	 */
	public String getPkColumn() {
		return pkColumn;
	}

	/**
	 * @param pkColumn the pkColumn to set
	 */
	public void setPkColumn(String pkColumn) {
		this.pkColumn = pkColumn;
	}

	/**
	 * @return the newIdentifierColumn
	 */
	public String getNewIdentifierColumn() {
		return newIdentifierColumn;
	}

	/**
	 * @param newIdentifierColumn the newIdentifierColumn to set
	 */
	public void setNewIdentifierColumn(String newIdentifierColumn) {
		this.newIdentifierColumn = newIdentifierColumn;
	}

	/**
	 * @return the newIdentifierValue
	 */
	public String getNewIdentifierValue() {
		return newIdentifierValue;
	}

	/**
	 * @param newIdentifierValue the newIdentifierValue to set
	 */
	public void setNewIdentifierValue(String newIdentifierValue) {
		this.newIdentifierValue = newIdentifierValue;
	}

	/**
	 * @return the ignoreUnchangedRecords
	 */
	public boolean isIgnoreUnchangedRecords() {
		return ignoreUnchangedRecords;
	}

	/**
	 * @param ignoreUnchangedRecords the ignoreUnchangedRecords to set
	 */
	public void setIgnoreUnchangedRecords(boolean ignoreUnchangedRecords) {
		this.ignoreUnchangedRecords = ignoreUnchangedRecords;
	}

	/**
	 * @return the sharedConnectionName
	 */
	public String getSharedConnectionName() {
		return sharedConnectionName;
	}

	/**
	 * @param sharedConnectionName the sharedConnectionName to set
	 */
	public void setSharedConnectionName(String sharedConnectionName) {
		this.sharedConnectionName = sharedConnectionName;
	}
	
	/**
	 * @return the treatEmptyAsNull
	 */
	public boolean isTreatEmptyAsNull() {
		return treatEmptyAsNull;
	}
	
	/**
	 * @param treatEmptyAsNull the treatEmptyAsNull to set
	 */
	public void setTreatEmptyAsNull(boolean treatEmptyAsNull) {
		this.treatEmptyAsNull = treatEmptyAsNull;
	}
	
	/**
	 * @return the properties
	 */
	public Properties getProperties() {
		return properties;
	}
	
	/**
	 * @param properties the properties to set
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	/**
	 * Set jdbc property for connection.
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, String value) {
		properties.put(key, value);
	}

	/**
	 * @return the batchSize
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * @param batchSize the batchSize to set
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * @return the skipError
	 */
	public boolean isSkipError() {
		return skipError;
	}

	/**
	 * @param skipError the skipError to set
	 */
	public void setSkipError(boolean skipError) {
		this.skipError = skipError;
	}

	/**
	 * @return the autoCreateTable
	 */
	public boolean isAutoCreateTable() {
		return autoCreateTable;
	}

	/**
	 * @param autoCreateTable the autoCreateTable to set
	 */
	public void setAutoCreateTable(boolean autoCreateTable) {
		this.autoCreateTable = autoCreateTable;
	}

	/**
	 * @return the autoDetectColumnTypes
	 */
	public boolean isAutoDetectColumnTypes() {
		return autoDetectColumnTypes;
	}

	/**
	 * @param autoDetectColumnTypes the autoDetectColumnTypes to set
	 */
	public void setAutoDetectColumnTypes(boolean autoDetectColumnTypes) {
		this.autoDetectColumnTypes = autoDetectColumnTypes;
	}

	/**
	 * @return the autoAlterColumns
	 */
	public boolean isAutoAlterColumns() {
		return autoAlterColumns;
	}

	/**
	 * @param autoAlterColumns the autoAlterColumns to set
	 */
	public void setAutoAlterColumns(boolean autoAlterColumns) {
		this.autoAlterColumns = autoAlterColumns;
	}
	
	/**
	 * @return the withTablock
	 */
	public boolean isWithTablock() {
		return withTablock;
	}

	/**
	 * @param withTablock the withTablock to set
	 */
	public void setWithTablock(boolean withTablock) {
		this.withTablock = withTablock;
	}

	/**
	 * @return the commitOnProcessFinished
	 */
	public boolean isCommitOnProcessFinished() {
		return commitOnProcessFinished;
	}

	/**
	 * @param commitOnProcessFinished the commitOnProcessFinished to set
	 */
	public void setCommitOnProcessFinished(boolean commitOnFinished) {
		this.commitOnProcessFinished = commitOnFinished;
	}

	/**
	 * @return the identifierSeparator
	 */
	public String getIdentifierSeparator() {
		return is;
	}

	/**
	 * Set the separator.
	 * @param identifierSeparator the identifierSeparator to set
	 */
	public void setIdentifierSeparator(String identifierSeparator) {
		this.is = identifierSeparator;
	}

	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * @return the autoIncrementColumn
	 */
	public String getAutoIncrementColumn() {
		return autoIncrementColumn;
	}

	/**
	 * @param autoIncrementColumn the autoIncrementColumn to set
	 */
	public void setAutoIncrementColumn(String autoIncrementColumn) {
		this.autoIncrementColumn = autoIncrementColumn;
	}

	/**
	 * @deprecated use {@link #getAutoIncrementColumn()}
	 * @return the replaceOnAutoIncrement
	 */
	public String getReplaceOnAutoIncrementColumn() {
		return autoIncrementColumn;
	}

	/**
	 * @deprecated use {@link #setAutoIncrementColumn(String)}
	 * @param replaceOnAutoIncrement the replaceOnAutoIncrement to set
	 */
	public void setReplaceOnAutoIncrementColumn(String replaceOnAutoIncrement) {
		this.autoIncrementColumn = replaceOnAutoIncrement;
	}

	/**
	 * @deprecated
	 * @return the replaceOnColumns
	 */
	public String[] getReplaceOnColumns() {
		return onColumns;
	}

	/**
	 * @deprecated
	 * @param replaceOnColumns the replaceOnColumns to set
	 */
	public void setReplaceOnColumns(String... replaceOnColumns) {
		this.onColumns = replaceOnColumns;
	}

	/**
	 * @return the onColumnsLastMaxValue
	 */
	public Object getOnColumnsLastMaxValue() {
		return onColumnsLastMaxValue;
	}

	/**
	 * @deprecated use {@link #getReplaceOnLastMaxValue()}
	 * @return the lastReplaceOnMaxId
	 */
	public Object getLastReplaceOnMaxId() {
		return onColumnsLastMaxValue;
	}
	
	/**
	 * @return the autoIncremenMaxValue
	 */
	public Object getOnColumnsMaxValue() {
		return onColumnsMaxValue;
	}

	/**
	 * @deprecated use {@link #getOnColumnsMaxValue()}
	 * @return the replaceOnMaxId
	 */
	public Object getReplaceOnMaxId() {
		return onColumnsMaxValue;
	}

	/**
	 * @deprecated
	 * @return the replaceOnColumnsOnProcessFinished
	 */
	public boolean isReplaceOnColumnsOnProcessFinished() {
		return onColumnsOnProcessFinished;
	}

	/**
	 * @deprecated
	 * @param replaceOnColumnsOnProcessFinished the replaceOnColumnsOnProcessFinished to set
	 */
	public void setReplaceOnColumnsOnProcessFinished(boolean replaceOnColumnsOnProcessFinished) {
		this.onColumnsOnProcessFinished = replaceOnColumnsOnProcessFinished;
	}

	/**
	 * @deprecated
	 * @return the replaceOnColumnsNullValue
	 */
	public String[] getReplaceOnColumnsNullValue() {
		return onColumnsNullValue;
	}

	/**
	 * @deprecated
	 * @param replaceOnColumnsNullValue the replaceOnColumnsNullValue to set
	 */
	public void setReplaceOnColumnsNullValue(String... replaceOnColumnsNullValue) {
		this.onColumnsNullValue = replaceOnColumnsNullValue;
	}

	/**
	 * @deprecated
	 * @return the replaceOnColumnsCollate
	 */
	public String[] getReplaceOnColumnsCollate() {
		return onColumnsCollate;
	}

	/**
	 * @deprecated
	 * @param replaceOnColumnsCollate the replaceOnColumnsCollate to set
	 */
	public void setReplaceOnColumnsCollate(String... replaceOnColumnsCollate) {
		this.onColumnsCollate = replaceOnColumnsCollate;
	}

	/**
	 * @return the deleteTable
	 */
	public boolean isDeleteTable() {
		return deleteTable;
	}

	/**
	 * @param deleteTable the deleteTable to set
	 */
	public void setDeleteTable(boolean deleteTable) {
		this.deleteTable = deleteTable;
	}

	/**
	 * @return the autoDetectColumnTypesRunning
	 */
	public boolean isAutoDetectColumnTypesRunning() {
		return autoDetectColumnTypesRunning;
	}

	/**
	 * @return the sqlDialect
	 */
	public SqlDialect getSqlDialect() {
		return sqlDialect;
	}

	/**
	 * @param sqlDialect the sqlDialect to set
	 */
	public void setSqlDialect(SqlDialect sqlDialect) {
		this.sqlDialect = sqlDialect;
		setTableName(tablename);
	}

	/**
	 * @return the enableObjectAlias
	 */
	public boolean isEnableObjectAlias() {
		return enableObjectAlias;
	}

	/**
	 * @param enableObjectAlias the enableObjectAlias to set
	 */
	public void setEnableObjectAlias(boolean enableObjectAlias) {
		this.enableObjectAlias = enableObjectAlias;
	}

	/**
	 * @return the enableGlobalObjectAlias
	 */
	public boolean isEnableGlobalObjectAlias() {
		return enableGlobalObjectAlias;
	}

	/**
	 * @param enableGlobalObjectAlias the enableGlobalObjectAlias to set
	 */
	public void setEnableGlobalObjectAlias(boolean enableGlobalObjectAlias) {
		this.enableGlobalObjectAlias = enableGlobalObjectAlias;
	}

	/**
	 * @return the objectAliasMaxLength
	 */
	public int getObjectAliasMaxLength() {
		return objectAliasMaxLength;
	}

	/**
	 * @param objectAliasMaxLength the objectAliasMaxLength to set
	 */
	public void setObjectAliasMaxLength(int objectAliasMaxLength) {
		this.objectAliasMaxLength = objectAliasMaxLength;
	}

	/**
	 * @return
	 */
	protected boolean isSimulatePkIdentity() {
		return simulatePkIdentity && sqlDialect == SqlDialect.ORACLE;
	}

	/**
	 * @deprecated use {@link #isOnColumnsOnProcessFinished()}
	 * @return the updateOnColumnsOnProcessFinished
	 */
	public boolean isUpdateOnColumnsOnProcessFinished() {
		return onColumnsOnProcessFinished;
	}

	/**
	 * @deprecated use {@link #setOnColumnsOnProcessFinished(boolean)}
	 * @param updateOnColumnsOnProcessFinished the updateOnColumnsOnProcessFinished to set
	 */
	public void setUpdateOnColumnsOnProcessFinished(boolean updateOnColumnsOnProcessFinished) {
		this.onColumnsOnProcessFinished = updateOnColumnsOnProcessFinished;
	}

	/**
	 * @deprecated use {@link #getOnColumns()}
	 * @return the updateOnColumns
	 */
	public String[] getUpdateOnColumns() {
		return onColumns;
	}

	/**
	 * @deprecated use {@link #setOnColumns(String...)}
	 * @param updateOnColumns the updateOnColumns to set
	 */
	public void setUpdateOnColumns(String... updateOnColumns) {
		this.onColumns = updateOnColumns;
	}

	/**
	 * @deprecated use {@link #getOnColumnsNullValue()}
	 * @return the updateOnColumnsNullValue
	 */
	public String[] getUpdateOnColumnsNullValue() {
		return onColumnsNullValue;
	}

	/**
	 * @deprecated use {@link #setOnColumnsNullValue(String...)}
	 * @param updateOnColumnsNullValue the updateOnColumnsNullValue to set
	 */
	public void setUpdateOnColumnsNullValue(String... updateOnColumnsNullValue) {
		this.onColumnsNullValue = updateOnColumnsNullValue;
	}

	/**
	 * @deprecated use {@link #getOnColumnsCollate()}
	 * @return the updateOnColumnsCollate
	 */
	public String[] getUpdateOnColumnsCollate() {
		return onColumnsCollate;
	}

	/**
	 * @deprecated use {@link #setOnColumnsCollate(String...)}
	 * @param updateOnColumnsCollate the updateOnColumnsCollate to set
	 */
	public void setUpdateOnColumnsCollate(String... updateOnColumnsCollate) {
		this.onColumnsCollate = updateOnColumnsCollate;
	}

	/**
	 * @deprecated use {@link #getOnColumnsLastMaxValue()}
	 * @return the updateOnLastMaxValue
	 */
	public Object getUpdateOnLastMaxValue() {
		return onColumnsLastMaxValue;
	}

	/**
	 * @return the onColumnsMode
	 */
	public OnColumnsMode getOnColumnsMode() {
		return onColumnsMode;
	}

	/**
	 * @param onColumnsMode the onColumnsMode to set
	 */
	public void setOnColumnsMode(OnColumnsMode onColumnsMode) {
		this.onColumnsMode = onColumnsMode;
	}

	/**
	 * @return the onColumnsOnProcessFinished
	 */
	public boolean isOnColumnsOnProcessFinished() {
		return onColumnsOnProcessFinished;
	}

	/**
	 * @param onColumnsOnProcessFinished the onColumnsOnProcessFinished to set
	 */
	public void setOnColumnsOnProcessFinished(boolean onColumnsOnProcessFinished) {
		this.onColumnsOnProcessFinished = onColumnsOnProcessFinished;
	}

	/**
	 * @return the onColumns
	 */
	public String[] getOnColumns() {
		return onColumns;
	}

	/**
	 * @param onColumns the onColumns to set
	 */
	public void setOnColumns(String... onColumns) {
		this.onColumns = onColumns;
	}

	/**
	 * @return the onColumnsNullValue
	 */
	public String[] getOnColumnsNullValue() {
		return onColumnsNullValue;
	}

	/**
	 * @param onColumnsNullValue the onColumnsNullValue to set
	 */
	public void setOnColumnsNullValue(String... onColumnsNullValue) {
		this.onColumnsNullValue = onColumnsNullValue;
	}

	/**
	 * @return the onColumnsCollate
	 */
	public String[] getOnColumnsCollate() {
		return onColumnsCollate;
	}

	/**
	 * @param onColumnsCollate the onColumnsCollate to set
	 */
	public void setOnColumnsCollate(String... onColumnsCollate) {
		this.onColumnsCollate = onColumnsCollate;
	}

	/**
	 * @return the onColumnsExclude
	 */
	public String[] getOnColumnsExclude() {
		return onColumnsExclude;
	}

	/**
	 * @param onColumnsExclude the onColumnsExclude to set
	 */
	public void setOnColumnsExclude(String... onColumnsExclude) {
		this.onColumnsExclude = onColumnsExclude;
	}

	/**
	 * @return Collection of DbColumnDef 
	 */
	public Collection<DbColumnDef> getColumns() {
		return columns.values();
	}

	/**
	 * @return the preventNotNullError
	 */
	public boolean isPreventNotNullError() {
		return preventNotNullError;
	}

	/**
	 * @param preventNotNullError the preventNotNullError to set
	 */
	public void setPreventNotNullError(boolean preventNotNullError) {
		this.preventNotNullError = preventNotNullError;
	}
	
	/**
	 * @return the sum of insertCount + updateCount + deleteCount
	 */
	public long getRecordsChangeCount() {
		return insertCount + updateCount + deleteCount;
	}

	/**
	 * @return the ignoreMissingSourceColumns
	 */
	public boolean isIgnoreMissingSourceColumns() {
		return ignoreMissingSourceColumns;
	}

	/**
	 * @param ignoreMissingSourceColumns the ignoreMissingSourceColumns to set
	 */
	public void setIgnoreMissingSourceColumns(boolean ignoreMissingSourceColumns) {
		this.ignoreMissingSourceColumns = ignoreMissingSourceColumns;
	}

	/**
	 * @return the insertCount
	 */
	public long getInsertCount() {
		return insertCount;
	}

	/**
	 * @param insertCount the insertCount to set
	 */
	public void setInsertCount(long insertCount) {
		this.insertCount = insertCount;
	}

	/**
	 * @return the updateCount
	 */
	public long getUpdateCount() {
		return updateCount;
	}

	/**
	 * @param updateCount the updateCount to set
	 */
	public void setUpdateCount(long updateCount) {
		this.updateCount = updateCount;
	}

	/**
	 * @return the deleteCount
	 */
	public long getDeleteCount() {
		return deleteCount;
	}

	/**
	 * @param deleteCount the deleteCount to set
	 */
	public void setDeleteCount(long deleteCount) {
		this.deleteCount = deleteCount;
	}

	/**
	 * @return the onColumnsType
	 */
	public OnColumnsType getOnColumnsType() {
		return onColumnsType;
	}

	/**
	 * @param onColumnsType the onColumnsType to set
	 */
	public void setOnColumnsType(OnColumnsType onColumnsType) {
		this.onColumnsType = onColumnsType;
	}

	/**
	 * @return the schemaname
	 */
	public String getSchemaName() {
		return schemaname;
	}

	/**
	 * @param schemaname the schemaname to set
	 */
	public void setSchemaName(String schemaname) {
		this.schemaname = schemaname;
	}

	/**
	 * @return the onColumnsIncludeMissingTargetColumns
	 */
	public boolean isOnColumnsIncludeMissingTargetColumns() {
		return onColumnsIncludeMissingTargetColumns;
	}

	/**
	 * @param onColumnsIncludeMissingTargetColumns the onColumnsIncludeMissingTargetColumns to set
	 */
	public void setOnColumnsIncludeMissingTargetColumns(boolean onColumnsIncludeMissingTargetColumns) {
		this.onColumnsIncludeMissingTargetColumns = onColumnsIncludeMissingTargetColumns;
	}

	/**
	 * @return the addColumnsToDataSet
	 */
	public boolean isAddColumnsToDataSet() {
		return addColumnsToDataSet;
	}

	/**
	 * @param addColumnsToDataSet the addColumnsToDataSet to set
	 */
	public void setAddColumnsToDataSet(boolean addColumnsToDataSet) {
		this.addColumnsToDataSet = addColumnsToDataSet;
	}

	/**
	 * @return the autoDetectColumnTypesNullable
	 */
	public boolean isAutoDetectColumnTypesNullable() {
		return autoDetectColumnTypesNullable;
	}

	/**
	 * @param autoDetectColumnTypesNullable the autoDetectColumnTypesNullable to set
	 */
	public void setAutoDetectColumnTypesNullable(boolean autoDetectColumnTypesNullable) {
		this.autoDetectColumnTypesNullable = autoDetectColumnTypesNullable;
	}

	/**
	 * @return the autoAlterColumnsMode
	 */
	public AutoAlterColumnsMode getAutoAlterColumnsMode() {
		return autoAlterColumnsMode;
	}

	/**
	 * @param autoAlterColumnsMode the autoAlterColumnsMode to set
	 */
	public void setAutoAlterColumnsMode(AutoAlterColumnsMode autoAlterColumnsMode) {
		this.autoAlterColumnsMode = autoAlterColumnsMode;
	}

	@Override
	public String toString() {
		return "JDBCLoader[" + tablename + "]";
	}

	/**
	 * @return the reopenClosedConnection
	 */
	public boolean isReopenClosedConnection() {
		return reopenClosedConnection;
	}

	/**
	 * @param reopenClosedConnection the reopenClosedConnection to set
	 */
	public void setReopenClosedConnection(boolean reopenClosedConnection) {
		this.reopenClosedConnection = reopenClosedConnection;
	}
	
}
