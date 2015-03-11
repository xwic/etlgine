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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
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

    //RPF: Trying to implement schema definitions in JDBC Loader
    private String schemaName = null;

	private String tablename = null;
	private String originalTablename = null;
	private boolean enableObjectAlias = true;
	private boolean enableGlobalObjectAlias = true;
	private int objectAliasMaxLength = 30;

	private boolean autoCreateTable = false;
	private boolean autoCreateColumns = false;
	private boolean autoDetectColumnTypes = false;
	private boolean autoDetectColumnTypesRunning = false;
	private boolean autoAlterColumns = false;	
	private boolean autoDataTruncate = false;
	private boolean commitOnProcessFinished = true;
	
	private boolean ignoreMissingTargetColumns = false;
	private boolean treatEmptyAsNull = false;
	private boolean truncateTable = false;
	private boolean deleteTable = false;
	private boolean tablePurged = false;
	private boolean skipError = false;
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
	
	private long insertCount = 0;
	private long updateCount = 0;
		
	private Connection connection = null;
	private PreparedStatement psInsert = null;
	private PreparedStatement psUpdate = null;
	private Map<String, DbColumnDef> columns;
	private Set<String> ignoredColumns = new HashSet<String>();
	
	private boolean withTablock = false;
	private boolean simulatePkIdentity = true;
	private String pkSequence = null;
	
	private SqlDialect sqlDialect = SqlDialect.MSSQL;
	
	/** custom jdbc properties */
	private Properties properties = new Properties();
	
	private String is = null;
	
	private boolean replaceOnColumnsOnProcessFinished = false;
	private String replaceOnAutoIncrementColumn = "Id";
	private String[] replaceOnColumns = null;
	private Object replaceOnMaxId = null;
	private Object lastReplaceOnMaxId = null;
	private String[] replaceOnColumnsNullValue = null;
	private String[] replaceOnColumnsCollate = null;
	
	private Map<String, String> objectAliasByName = null;
	private Map<String, String> objectNameByAlias = null;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(pkColumn, "PkColumn must be specified for UPDATE mode.");
		}
		if (mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(newIdentifierColumn, "NewIdentifierColumn must be specified for INSERT_OR_UPDATE mode.");
		}
		
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
				
				
				properties.setProperty("user", username);
				properties.setProperty("password", password);
				
				connection = DriverManager.getConnection(connectionUrl, properties);
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		} else {
			monitor.logInfo("Using named connection: " + connectionName);
			if (batchSize == -1) {
				batchSize = JDBCUtil.getBatchSize(processContext, connectionName);
			}
			try {
				if (sharedConnectionName != null) {
					connection = JDBCUtil.getSharedConnection(processContext, sharedConnectionName, connectionName);
				} else {
					connection = JDBCUtil.openConnection(processContext, connectionName);
				}
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		}
		
		if (is == null) {
			is = JDBCUtil.getIdentifierSeparator(connection);
		}

		tablePurged = false;

		/* 
		 * register JDBCLoader also as first Transformer to execute bulk updates before other Transformers
		 * to ensure all records are committed.
		 */
		
		IProcess iProcess = processContext.getProcess();
		if (iProcess instanceof ETLProcess) {
			ETLProcess etlProcess = (ETLProcess)iProcess;
			ITransformer transformer = new AbstractTransformer() {
				@Override
				public void postSourceProcessing(IProcessContext processContext) throws ETLException {
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
		
		if (replaceOnColumnsNullValue != null && replaceOnColumns != null && replaceOnColumnsNullValue.length != 1 && replaceOnColumnsNullValue.length != replaceOnColumns.length) {
			
			processContext.getMonitor().logWarn("Replace on column configuration inconsistent on table " + getTablenameQuoted());
		}
	}
	
	@Override
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {
		super.postSourceProcessing(processContext);
		
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

			if (!replaceOnColumnsOnProcessFinished) {
				// if replace on columns is enabled and records existed, ensure consistency 
				try {
					executeDeleteForReplace();
				} catch (SQLException e) {
					throw new ETLException(e);
				}
			}			
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
		
		if (connection != null) {
			try {
				// check open batch statements
				executeBatch();

				if (replaceOnColumnsOnProcessFinished) {
					// if replace on columns is enabled and records existed, ensure consistency 
					try {
						executeDeleteForReplace();
					} catch (SQLException e) {
						throw new ETLException(e);
					}
				}			

				if (sharedConnectionName == null) {
					// only close the connection if it is not shared!
					monitor.logInfo("JDBCLoader close connection");
					connection.close();
				} else {
					if (commitOnProcessFinished && !connection.getAutoCommit()) {
						// commit for shared connections
						monitor.logInfo("JDBCLoader commit transaction");
						connection.commit();
					}
				}
			} catch (SQLException e) {
				throw new ETLException("Error closing connection: " + e, e);
			}
			connection = null;
		}

		monitor.logInfo("JDBCLoader " + insertCount + " records inserted, " + updateCount + " records updated.");
	}
	
	/**
	 * Deletes the records that had been inserted.
	 * @throws SQLException
	 */
	protected void executeDeleteForReplace() throws SQLException {
		// exit if not applicable
		if (autoDetectColumnTypesRunning) {
			return;
		}
		
		// check for replace
		if (replaceOnMaxId != null) {
			lastReplaceOnMaxId = null;
			StringBuilder sql = new StringBuilder("delete t\n");
			sql
			.append("from " + getTablenameQuoted() + " t\n")
			.append("inner join (\n")
			.append("select distinct ");
			StringBuilder on = new StringBuilder();
			for (int i = 0; i < replaceOnColumns.length; i++) {
				String col = replaceOnColumns[i];
				String nul = replaceOnColumnsNullValue != null ? 
						replaceOnColumnsNullValue.length > i ? replaceOnColumnsNullValue[i] : 
						replaceOnColumnsNullValue.length == 1 ? replaceOnColumnsNullValue[0] : null
						: null;
				String collate = replaceOnColumnsCollate != null && replaceOnColumnsCollate.length > i ? replaceOnColumnsCollate[i] : null;
				if (on.length() > 0) {
					sql.append(", ");
					on.append(" and ");
				}
				col = is + col + is;
				if (nul == null) {
					// default behavior
					sql.append(col);
					if (collate != null) {
						sql.append(" ").append(collate).append(" as ").append(col);
					}
					on.append("n.").append(col).append(" = t.").append(col);
				} else {
					// use coalesce on null values
					sql.append("coalesce(").append(col).append(",").append(nul).append(")");
					if (collate != null) {
						sql.append(" ").append(collate);
					}
					sql.append(" as ").append(col);
					on.append("n.").append(col).append(" = coalesce(t.").append(col).append(",").append(nul).append(")");
				}
			}
			sql.append(" from " + getTablenameQuoted() + " where " + is + replaceOnAutoIncrementColumn + is + " > ?\n")
			.append(") n on ").append(on).append("\n")
			.append("where t.").append(is).append(replaceOnAutoIncrementColumn).append(is).append(" <= ?");

			PreparedStatement ps = connection.prepareStatement(sql.toString());
			ps.setObject(1, replaceOnMaxId);
			ps.setObject(2, replaceOnMaxId);
			try {
				monitor.logInfo("JDBCLoader executes delete statement on table " + getTablenameQuoted()); 
				int cnt = ps.executeUpdate();
				monitor.logInfo("JDBCLoader " + cnt + " records deleted.");
			} finally {
				lastReplaceOnMaxId = replaceOnMaxId;
				// clear max id
				replaceOnMaxId = null;
				ps.close();
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {
		super.preSourceProcessing(processContext);
		
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
			checkTableStructure();
			
			if (truncateTable && !tablePurged) {
				// truncate table only once for source processing, set to false in method initialize
				truncateTable();
			}
			
			if (deleteTable && !tablePurged) {
				// delete from table only once for source processing, set to false in method initialize
				deleteTable();
			}
			
			// build prepared statement.. and Update statement.
			buildPreparedStatements();
			
			// set offset for batch insert/update
			batchRecordsCountOffset = processContext.getRecordsCount();
			
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
		
		sql.append(command).append(getTablenameQuoted());
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
			
			if (colDef.getColumn() != null || (simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn))) {
				
				// ignore identity columns for insert and update (identity is not support on ORACLE)
				// eugen - SQL server supports identity so the last part of the check is not needed - || (!simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn) ) 
				if (colDef.getTypeName().toLowerCase().indexOf("identity") != -1) {
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
					sql.append(is).append(colDef.getName()).append(is);
					if (simulatePkIdentity && pkColumn != null && colDef.getName().equalsIgnoreCase(pkColumn)) {
						sqlValues.append(is).append(pkSequence).append(is).append(".NEXTVAL");
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
						sql.append(is).append(colDef.getName()).append(is).append(" = ?");
					}
				}
			}
		}
		
		if (insert) {
			
			sqlValues.append(")");
			sql.append(") VALUES").append(sqlValues);
			
		} else {		
			
			sql.append(" WHERE ").append(is).append(pkColumn).append(is).append(" = ?");
		}
		
		return sql.toString();
	}
	
	/**
	 * Build prepared SQL statements
	 * @throws SQLException 
	 */
	protected void buildPreparedStatements() throws SQLException {
		String sql = buildPreparedStatement(tablename, columns.values(), Mode.INSERT, pkColumn != null ? pkColumn : "Id", pkSequence); 
		String sqlUpd = buildPreparedStatement(tablename, columns.values(), Mode.UPDATE, pkColumn, null);

		for (DbColumnDef colDef : columns.values()) {

			if (colDef.getColumn() == null) {
				if (!ignoredColumns.contains(colDef.getName())) {
					monitor.logWarn("A column in the target table does not exist in the source and is skipped (" + colDef.getName() + ")");
				}
			}
		}
		
		if (mode == Mode.INSERT || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("INSERT Statement: " + sql);
			psInsert = connection.prepareStatement(sql /*, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY*/);
		}
		
		if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("UPDATE Statement: " + sqlUpd);
			psUpdate = connection.prepareStatement(sqlUpd /*, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY*/);
		}
	}

	/**
	 * Checks if table exists and auto creates it if autoCreateTable is true.
	 * @return DatabaseMetaData
	 * @throws SQLException 
	 * @throws ETLException
	 */
	protected DatabaseMetaData checkTableExists() throws SQLException, ETLException {
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet rs = metaData.getTables(catalogname == null ? connection.getCatalog() : catalogname, getSchemaName(), tablename, null);
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
				if (mode == Mode.INSERT && replaceOnColumns != null && replaceOnColumns.length > 0 && (!replaceOnColumnsOnProcessFinished || replaceOnMaxId == null)) {
					// get max auto increment id for replace
					Statement stmt = connection.createStatement();
					ResultSet rs_max = stmt.executeQuery("select max(" + is + replaceOnAutoIncrementColumn + is + ") from " + getTablenameQuoted());
					rs_max.next();
					replaceOnMaxId = rs_max.getObject(1);
					rs_max.close();
					stmt.close();
					if (replaceOnMaxId != null) {
						StringBuilder cols = new StringBuilder();
						for (String col : replaceOnColumns) {
							if (cols.length() > 0) {
								cols.append(", ");
							}
							cols.append("[").append(col).append("]");
						}
						monitor.logInfo("Using max(" + is + replaceOnAutoIncrementColumn + is + ") = " + replaceOnMaxId + " to replace records on columns " + cols);
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
	protected void checkTableStructure() throws ETLException, SQLException {
		
		tablename = getObjectAlias(null, tablename);
		if (originalTablename != null && !originalTablename.equals(tablename)) {
			monitor.logWarn("JDBCLoader uses tablename alias " + is + tablename + is + " for originally configured tablename " + is + originalTablename + is);
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
			
			if (!column.isExclude()) {
				DbColumnDef dbcd = columns.get(column.computeTargetName().toUpperCase());
				if (dbcd == null) {
					// try column name
					dbcd = columns.get(column.getName().toUpperCase());
				}
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
						monitor.logWarn("JDBCLoader uses column alias " + is + dbcd.getName() + is + " for originally configured column " + is + originalColumnName + is);
					}
				}
				if (dbcd != null) {
					dbcd.setColumn(column);
				} else {
					processContext.getMonitor().logWarn("Column does not exist: " + is + column.computeTargetName() + is);
					missingCols.add(column);
				}
			}
		}
		if (missingCols.size() > 0) {
			if (autoCreateColumns) {
				if (autoDetectColumnTypes && !(processContext.getCurrentSource() instanceof JDBCSource)) {
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

	/**
	 * 
	 * @param metaData
	 * @param tablename
	 * @return
	 * @throws SQLException 
	 */
	private Map<String, DbColumnDef> loadColumns(DatabaseMetaData metaData, String tablename) throws SQLException {

		ResultSet rs = metaData.getColumns(catalogname == null ? connection.getCatalog() : catalogname, getSchemaName(), tablename, null);
		try {
			//dumpResultSet(rs);
			Map<String, DbColumnDef> columns = new LinkedHashMap<String, DbColumnDef>();
			while (rs.next()) {
				String name = rs.getString("COLUMN_NAME");
				int type = rs.getInt("DATA_TYPE");
				int size = rs.getInt("COLUMN_SIZE");
				String allowNull = rs.getString("NULLABLE");
				String typeName = rs.getString("TYPE_NAME");
				DbColumnDef colDef = new DbColumnDef(name, type, typeName, size, allowNull.equals("YES") || allowNull.equals("1") || allowNull.equals("TRUE"));
				columns.put(name.toUpperCase(), colDef);
			}
			return columns;
		} finally {
			rs.close();
		}
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
			}
			
			Map<IColumn, ColumnType> columnTypes = new HashMap<IColumn, ColumnType>();
			
			SimpleDateFormat[] dateFormat = new SimpleDateFormat[] {
				new SimpleDateFormat("MM/DD/yyyy"),
				new SimpleDateFormat("DD-MMM-yyyy"),
				new SimpleDateFormat("yyyy-MM-DD"),
				new SimpleDateFormat("MM/DD/yy"),
				new SimpleDateFormat("yyyy-MM-DD HH:mm:ss.S"),
			};
			
			// assume source is opened already, iterate all records
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
									Date date = format.parse(s);
									if (format.format(date).length() == s.length()) {
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
				transformer.postSourceProcessing(processContext);
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
					dbColumnsDef.add(getDbColumnDef(tablename, "PARENT", DataType.STRING, 128));
					dbColumnsDef.add(getDbColumnDef(tablename, "NAME", DataType.STRING, 128));
					dbColumnsDef.add(getDbColumnDef(tablename, "ALIAS", DataType.STRING, objectAliasMaxLength));
					createTable(tablename, dbColumnsDef);
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
			String tag = i == 0 ? "…" : "…" + i + "…";
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
		String sql = buildPreparedStatement(tablename, columns.values(), Mode.INSERT, "Id", pkSequence);
		PreparedStatement ps = connection.prepareStatement(sql);
		Record record = new Record(null);
		record.setData(colParent, parentName);
		record.setData(colName, name);
		record.setData(colAlias, alias);		
		try {
			doInsert(processContext, record, columns.values(), ps, "Id");
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
		
		boolean defaultInvocation = tablename == this.tablename;
		
		processContext.getMonitor().logInfo("Creating missing table: " + tablename);

		Statement stmt = connection.createStatement();
		StringBuilder sql = new StringBuilder();

		StringBuilder columnsDef = new StringBuilder();

		for (DbColumnDef dbcd : dbColumnsDef) {
			if (dbcd.getName().equals("Id")) {
				// skip default primary key column
				continue;
			}
			columnsDef.append(", ").append(is).append(getObjectAlias(originalTablename, dbcd.getName())).append(is).append(" ").append(dbcd.getTypeNameDetails());
		}
		
		switch (sqlDialect) {
		case MSSQL :
			sql.append("CREATE TABLE ").append(getTablenameQuoted(tablename)).append(" (");
			sql.append("Id [bigint] IDENTITY(1,1) NOT NULL").append(columnsDef).append(", CONSTRAINT PK_").append(tablename).append(" PRIMARY KEY (Id))");
			break;
		case ORACLE : 
			sql.append("CREATE TABLE ").append(getTablenameQuoted(tablename)).append(" (");
			sql.append(is).append("Id").append(is).append(" NUMBER(20) NOT NULL").append(columnsDef).append(", CONSTRAINT ").append(is).append(getObjectAlias(null, "PK_" + originalTablename)).append(is).append(" PRIMARY KEY (").append(is).append("Id").append(is).append("))");
			break;
		}
		
		processContext.getMonitor().logInfo("Creating missing table sql: \n" + sql.toString());
		
		stmt.execute(sql.toString());
		
		if (isSimulatePkIdentity()) {
			sql.setLength(0);
			String pkSequence = this.pkSequence != null ? this.pkSequence : getObjectAlias(null, "SEQ_" + originalTablename);
			sql.append("CREATE SEQUENCE " + is + pkSequence + is);
			processContext.getMonitor().logInfo("Creating missing sequence for primary key column on table " + getTablenameQuoted(tablename) + ":\n" + sql.toString());
			stmt.execute(sql.toString());
			if (defaultInvocation) {
				this.pkSequence = pkSequence;
			}
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
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + getTablenameQuoted());
				rs.next();
				rows = rs.getInt(1);
				stmt.executeUpdate("TRUNCATE TABLE " + getTablenameQuoted());
			} catch (SQLException e) {
				// try DELETE FROM
				rows = stmt.executeUpdate("DELETE FROM " + getTablenameQuoted());
			}
			processContext.getMonitor().logInfo("TRUNCATED TABLE " + getTablenameQuoted() + " - " + rows + " rows have been deleted.");
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
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + getTablenameQuoted());
			rs.next();
			int rows = rs.getInt(1);
			// try DELETE FROM
			rows = stmt.executeUpdate("DELETE FROM " + getTablenameQuoted());
			processContext.getMonitor().logInfo("DELETE FROM TABLE " + getTablenameQuoted() + " - " + rows + " rows have been deleted.");
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
	protected DbColumnDef getDbColumnDef(String tablename, String columnName, DataType typeHint, int lengthHint) throws SQLException {
		IColumn col = new Column(columnName);
		col.setTypeHint(typeHint);
		col.setLengthHint(lengthHint);
		return getDbColumnDef(tablename, col);
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
			typeName = DATETIME;
			dbcd.setType(Types.TIMESTAMP);
			break;
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
		case BOOLEAN:
			typeName = BIT;
			dbcd.setType(Types.BIT);
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
		
		dbcd.setAllowsNull(true);
		return dbcd;
	}

	/**
	 * @param missingCols
	 * @param columns 
	 * @throws SQLException 
	 */
	private void createColumns(List<IColumn> missingCols, Map<String, DbColumnDef> columns) throws SQLException {
        StringBuilder alterCommand = new StringBuilder();
        alterCommand.append("ALTER TABLE ");
        alterCommand.append(getTablenameQuoted());
        alterCommand.append(" ADD ");

		StringBuilder sql = new StringBuilder();
        sql.append(alterCommand);
		if (sqlDialect == SqlDialect.ORACLE) {
			sql.append("(");
		}
		boolean first = true;
		
		for (IColumn col : missingCols) {
			
			// if a column is used twice, we must make sure that it is not created twice as well!
			if (!columns.containsKey(col.computeTargetName())) {
				String originalColumnName = col.computeTargetName();
				DbColumnDef dbcd = getDbColumnDef(tablename, col);
				
				if (!originalColumnName.equals(dbcd.getName())) {
					monitor.logWarn("JDBCLoader uses column alias " + dbcd.getName() + " for originally configured column " + originalColumnName);
				}
				
				if (first) {
					first = false;
                } else if (SqlDialect.SQLITE.equals(sqlDialect)) {
                    sql.append(";");
                    sql.append(alterCommand);
				} else  {
					sql.append(", ");
				}


			    sql.append(is).append(dbcd.getName()).append(is).append(" ").append(dbcd.getTypeNameDetails());

				columns.put(dbcd.getName(), dbcd);
			}
			
		}
		if (sqlDialect == SqlDialect.ORACLE) {
			sql.append(")");
		}		
		processContext.getMonitor().logInfo("Creating missing columns: \n" + sql.toString());

        if (SqlDialect.SQLITE.equals(sqlDialect)) {
            String[] sqls = sql.toString().split(";");
            for(String sqlLite: sqls){
                Statement stmt = connection.createStatement();
                stmt.execute(sqlLite);
                stmt.close();
            }
        } else {
            Statement stmt = connection.createStatement();
            stmt.execute(sql.toString());
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
		if (sqlDialect == SqlDialect.MSSQL && catalogname != null) {
            //RPF: added schema support here, if exists it got added, otherwise using "dbo" (was before used directly here)
            sql.append(is).append(catalogname).append(is).append(".").append(is).append(getSchemaName() == null ? "dbo" : getSchemaName()).append(is).append(".");
		}
        //RPF: check schema, if exists add on quoted notation
        if (getSchemaName() != null) {
            sql.append(is).append(getSchemaName()).append(is).append(".");
        }

        sql.append(is).append(tablename).append(is);
		return sql.toString();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {

		try {
			switch (mode) {
			case INSERT:
				doInsert(processContext, record, columns.values(), psInsert, pkColumn != null ? pkColumn : "Id");
				break;
			case UPDATE:
				doUpdate(processContext, record);
				break;
			case INSERT_OR_UPDATE:
				if (Validate.equals(newIdentifierValue, record.getData(newIdentifierColumn))) {
					doInsert(processContext, record, columns.values(), psInsert, pkColumn);
				} else {
					doUpdate(processContext, record);
				}
			}
		} catch (Throwable t) {
			record.markInvalid(t.getLocalizedMessage());
			String msg = "Cannot process record " + processContext.getRecordsCount();
			if (skipError) {
				processContext.getMonitor().logError(msg , t);
			} else {
				throw new ETLException(msg, t);
			}
		}
		
	}
	
	/**
	 * Invoked after update statement received changed record data to allow customer flagging.
	 * Must return true to update the prepared statement again. 
	 * @param processContext
	 * @param record
	 * @return
	 * @throws ETLException
	 */
	protected boolean onRecordUpdated(IProcessContext processContext, IRecord record) throws ETLException {
		return false;
	}
	
	private void doUpdate(IProcessContext processContext, IRecord record) throws ETLException {
		boolean handleException = true;
		try {
			psUpdate.clearParameters();
			
			int idx = 1;
			boolean modified = false;
			DbColumnDef pkColDef = null;
			
			for (int i = 0; i < 2; i++) { 
				for (DbColumnDef colDef : columns.values()) {
					// Identity is not support on ORACLE
					if (colDef.getColumn() != null && !colDef.getName().equalsIgnoreCase(pkColumn) && colDef.getTypeName().toLowerCase().indexOf("identity") == -1) {
						
						Object value = record.getData(colDef.getColumn());
						setPSValue(psUpdate, idx++, value, colDef);
						
						modified = modified | record.isChanged(colDef.getColumn());
						
					}
	
					// PK might be excluded
					if (colDef.getName().equals(pkColumn)) {
						pkColDef = colDef;
					}
				}
				
				if (modified && onRecordUpdated(processContext, record)) {
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
				handleException(t, record, processContext.getRecordsCount(), true);
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
		if (value == null || (treatEmptyAsNull && value instanceof String && ((String)value).length() == 0)) {
			ps.setNull(idx, colDef.getType());
		} else {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.CLOB:
			case -15: //Types.NCHAR: <-- This does not exist in any java version prior to 1.6, so I use the hard coded value here.
			case -9: { //Types.NVARCHAR: <-- This does not exist in any java version prior to 1.6, so I use the hard coded value here.
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
							alterColumnType(colDef, Types.FLOAT);
							ps.setFloat(idx, f);
							break;
						} catch (NumberFormatException e) {}
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
							alterColumnType(colDef, Types.BIGINT);
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
							alterColumnType(colDef, Types.BIGINT);
							ps.setLong(idx, l);
							break;
						} catch (NumberFormatException e) {}
						try {
							float f = Float.parseFloat(value.toString());
							// data truncate sql exception might happen: alter pro-active the table
							// empty batch, TODO shared or open connections might be checked that could cause a looked situation
							executeBatch();
							alterColumnType(colDef, Types.FLOAT);
							ps.setFloat(idx, f);
							break;
						} catch (NumberFormatException e) {}
					}
					// add column information
					throw new ETLException(nfe.getMessage() + ", column '" + colDef.getName() + "'", nfe);
				}
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
			case Types.NUMERIC:
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
				} else {
					// unknown value
					ps.setObject(idx, value);
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
		// set length
		int newSize = 1;
		for (; newSize < size; newSize *= 2);
		
		String typeName = colDef.getTypeName();
		if (typeName == null) {
			// identify column type
			throw new SQLException("Missing column '" + colDef.getName() + "' typeName");
		}

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
					} else {
						processContext.getMonitor().logError("Max size for varchar is 4000, cannot apply new size " + newSize);
						return;
					}
					break;
				}
			}
			sb.append("ALTER TABLE ").append(getTablenameQuoted()).append(" MODIFY ").append(is);
			sb.append(colDef.getName()).append(is).append(" ").append(newType);
			break;
		}
		default: {
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
			sb.append("ALTER TABLE ").append(is).append(tablename).append(is).append(" ALTER COLUMN ").append(is);
			sb.append(colDef.getName()).append(is).append(" ").append(newType);
			break;
		}
		}
		
		processContext.getMonitor().logWarn("Alter column " + is + colDef.getName() + is + " size from " + colDef.getSize() + " to " + newSize);

		processContext.getMonitor().logInfo(sb.toString());
		
		// alter column
		Statement stmt = connection.createStatement();
		try {
			stmt.execute(sb.toString());
		} finally {
			stmt.close();
		}
		
		colDef.setSize(newSize);
		
	}

	/**
	 * Alter column to specified type (if supported by db).
	 * @param colDef
	 * @param type
	 * @throws SQLException
	 */
	protected void alterColumnType(DbColumnDef colDef, int type) throws SQLException {
		String newTypeName = null;
		switch (type) {
		case Types.BIGINT:
			newTypeName = "bigint";
			break;
		case Types.FLOAT:
			newTypeName = "float";
			break;
		default:
			throw new SQLException("Unsupported column type " + type + " for alter column " + colDef);
		}
		processContext.getMonitor().logWarn("Alter column '" + colDef.getName() + "' type from " + colDef.getTypeName() + " to " + newTypeName);

		String typeName = colDef.getTypeName();
		if (typeName == null) {
			// identify column type
			throw new SQLException("Missing column '" + colDef.getName() + "' typeName");
		}
		
		// alter column
		Statement stmt = connection.createStatement();
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ").append(getTablenameQuoted()).append(" ALTER COLUMN ").append(is);
		sb.append(colDef.getName()).append(is).append(" ").append(newTypeName);
		
		processContext.getMonitor().logInfo(sb.toString());
		
		stmt.execute(sb.toString());
		
		colDef.setType(type);
		colDef.setTypeName(newTypeName);
		
	}

	
	private void doInsert(IProcessContext processContext, IRecord record, Collection<DbColumnDef> columns, PreparedStatement ps, String pkColumn) throws ETLException {
		boolean handleException = true;
		boolean defaultInvocation = ps == psInsert; // if defaultInvocation is false, then batch insert is not used (only used for the "Oracle" dictionary "workaround")
		try {
			ps.clearParameters();
			
			int idx = 1;
			for (DbColumnDef colDef : columns) {
				if (colDef.getColumn() != null) {
					
					// ignore identity columns for insert and update (identity is not supported on ORACLE)
					if (colDef.getTypeName().toLowerCase().indexOf("identity") != -1 || (pkColumn != null && isSimulatePkIdentity() && colDef.getName().equalsIgnoreCase(pkColumn))) {
						continue;
					}
					
					Object value = record.getData(colDef.getColumn());
					setPSValue(ps, idx++, value, colDef);
					
				}
			}
			
			if (batchSize < 1 || !defaultInvocation) {
				// non-batched mode
				int count = ps.executeUpdate();
				if (count == -2) {
					/* For some unknown reason ASUPDW2 (Oracle, version: Oracle9i Enterprise Edition Release 9.2.0.4.0) 
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
				handleException(t, record, processContext.getRecordsCount(), true);
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
		if (record != null && (t instanceof DataTruncation || t.getCause() instanceof DataTruncation || t.getMessage().toLowerCase().contains("truncation"))) {
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
					batchRecords = batchInsertRecords;
					result = psInsert.executeBatch();
					for (int i = 0; i < result.length; i++) {
						int count = result[i];
						if (count == -2) {
							/* For some unknown reason ASUPDW2 (Oracle, version: Oracle9i Enterprise Edition Release 9.2.0.4.0) 
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
				}
				
				// batch update
				if (batchCountUpdate > 0) {
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
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * @param tablename the tablename to set
	 */
	public void setTablename(String tablename) {
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

    /**
     * Sets the schema name, can be null (default)
     *
     * @param schemaName
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the schema name, can be null
     */
    public String getSchemaName() {
        return schemaName;
    }

	/**
	 * @param catalogName the catalogName to set
	 */
	public void setCatalogName(String catalogName) {
		this.catalogname = catalogName;
	}

	/**
	 * @return the catalogName
	 */
	public String getCatalogName() {
		return catalogname;
	}

	/**
	 * @param catalogname the catalogname to set
	 */
	public void setCatalogname(String catalogname) {
		this.catalogname = catalogname;
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
	 * @return the replaceOnAutoIncrement
	 */
	public String getReplaceOnAutoIncrementColumn() {
		return replaceOnAutoIncrementColumn;
	}

	/**
	 * @param replaceOnAutoIncrement the replaceOnAutoIncrement to set
	 */
	public void setReplaceOnAutoIncrementColumn(String replaceOnAutoIncrement) {
		this.replaceOnAutoIncrementColumn = replaceOnAutoIncrement;
	}

	/**
	 * @return the replaceOnColumns
	 */
	public String[] getReplaceOnColumns() {
		return replaceOnColumns;
	}

	/**
	 * @param replaceOnColumns the replaceOnColumns to set
	 */
	public void setReplaceOnColumns(String... replaceOnColumns) {
		this.replaceOnColumns = replaceOnColumns;
	}

	/**
	 * @return the lastReplaceOnMaxId
	 */
	public Object getLastReplaceOnMaxId() {
		return lastReplaceOnMaxId;
	}
	

	/**
	 * @return the replaceOnMaxId
	 */
	public Object getReplaceOnMaxId() {
		return replaceOnMaxId;
	}

	/**
	 * @param replaceOnMaxId the replaceOnMaxId to set
	 */
	public void setReplaceOnMaxId(Object replaceOnMaxId) {
		this.replaceOnMaxId = replaceOnMaxId;
	}

	/**
	 * @return the replaceOnColumnsOnProcessFinished
	 */
	public boolean isReplaceOnColumnsOnProcessFinished() {
		return replaceOnColumnsOnProcessFinished;
	}

	/**
	 * @param replaceOnColumnsOnProcessFinished the replaceOnColumnsOnProcessFinished to set
	 */
	public void setReplaceOnColumnsOnProcessFinished(boolean replaceOnColumnsOnProcessFinished) {
		this.replaceOnColumnsOnProcessFinished = replaceOnColumnsOnProcessFinished;
	}

	/**
	 * @return the replaceOnColumnsNullValue
	 */
	public String[] getReplaceOnColumnsNullValue() {
		return replaceOnColumnsNullValue;
	}

	/**
	 * @param replaceOnColumnsNullValue the replaceOnColumnsNullValue to set
	 */
	public void setReplaceOnColumnsNullValue(String... replaceOnColumnsNullValue) {
		this.replaceOnColumnsNullValue = replaceOnColumnsNullValue;
	}

	/**
	 * @return the replaceOnColumnsCollate
	 */
	public String[] getReplaceOnColumnsCollate() {
		return replaceOnColumnsCollate;
	}

	/**
	 * @param replaceOnColumnsCollate the replaceOnColumnsCollate to set
	 */
	public void setReplaceOnColumnsCollate(String... replaceOnColumnsCollate) {
		this.replaceOnColumnsCollate = replaceOnColumnsCollate;
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
}
