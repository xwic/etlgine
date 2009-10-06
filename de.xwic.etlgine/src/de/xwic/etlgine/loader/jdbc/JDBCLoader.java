/**
 * 
 */
package de.xwic.etlgine.loader.jdbc;

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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.ITransformer;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.impl.DataSet;
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

	private static Logger log = Logger.getLogger(JDBCLoader.class.getName());
	
	public enum Mode {
		INSERT,
		UPDATE,
		INSERT_OR_UPDATE
	}
	
	private String sharedConnectionName = null;
	private String connectionName = null;
	// by default use the JTDS driver...
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl = null;
	private String username = null;
	private String password = null;
	private String tablename = null;

	private boolean autoCreateTable = false;
	private boolean autoCreateColumns = false;
	private boolean autoDetectColumnTypes = false;
	private boolean autoDataTruncate = false;
	
	private boolean ignoreMissingTargetColumns = false;
	private boolean treatEmptyAsNull = false;
	private boolean truncateTable = false;
	private boolean skipError = false;
	private int batchSize = -1;
	private int batchCountInsert = 0;
	private int batchCountUpdate = 0;
	
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
	
	/** custom jdbc properties */
	private Properties properties = new Properties();
	
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
				log.info("Using direct connection - URL: " + connectionUrl);
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
			log.info("Using named connection: " + connectionName);
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

		if (truncateTable) {
			try {
				checkTableExists();
			} catch (SQLException se) {
				throw new ETLException("Error initializing target database/tables: " + se, se);
			}
			truncateTable();
		}
		
//		try {
////			connection.setAutoCommit(false);
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}
	
	@Override
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {
		super.postSourceProcessing(processContext);
		
		if (connection != null) {
			try {
				// check open batch statements
				executeBatch();
			} catch (SQLException e) {
				throw new ETLException(e);
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
				
//				connection.commit();
				if (sharedConnectionName == null) {
					// only close the connection if it is not shared!
					connection.close();
				}
				
			} catch (SQLException e) {
				throw new ETLException("Error closing connection: " + e, e);
			}
			connection = null;
		}

		monitor.logInfo("JDBCLoader " + insertCount + " records inserted, " + updateCount + " records updated.");
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
			
			// check table structure, adds missing columns
			checkTableStructure();
			
			// build prepared statement.. and Update statement.
			buildPreparedStatements();
			
		} catch (SQLException se) {
			throw new ETLException("Error initializing target database/tables: " + se, se);
		}
	}
	
	/**
	 * Build prepared SQL statements
	 * @throws SQLException 
	 */
	protected void buildPreparedStatements() throws SQLException {
		StringBuilder sql = new StringBuilder();
		StringBuilder sqlValues = new StringBuilder();
		StringBuilder sqlUpd = new StringBuilder();
		sql.append("INSERT INTO [").append(tablename).append("] (");
		sqlUpd.append("UPDATE [").append(tablename).append("] SET ");
		sqlValues.append("(");
		
		boolean firstI = true;
		boolean firstU = true;
		for (DbColumnDef colDef : columns.values()) {
			if (colDef.getColumn() != null) {
				
				// INSERT Statement
				if (firstI) {
					firstI = false;
				} else {
					sql.append(", ");
					sqlValues.append(", ");
				}
				sql.append("[");
				sql.append(colDef.getName());
				sql.append("]");
				sqlValues.append("?");

				// UPDATE Statement (might skip pk)
				if (!colDef.getName().equalsIgnoreCase(pkColumn)) {
					if (firstU) {
						firstU = false;
					} else {
						sqlUpd.append(", ");
					}
					sqlUpd.append("[");
					sqlUpd.append(colDef.getName());
					sqlUpd.append("] = ?");
				}
			} else {
				if (!ignoredColumns.contains(colDef.getName())) {
					monitor.logWarn("A column in the target table does not exist in the source and is skipped (" + colDef.getName() + ")");
				}
			}
		}
		sqlValues.append(")");
		sql.append(") VALUES").append(sqlValues);
		
		sqlUpd.append(" WHERE [" + pkColumn + "] = ?");
		
		if (mode == Mode.INSERT || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("INSERT Statement: " + sql);
			psInsert = connection.prepareStatement(sql.toString());
		}
		
		if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
			monitor.logInfo("UPDATE Statement: " + sqlUpd);
			psUpdate = connection.prepareStatement(sqlUpd.toString());
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
		ResultSet rs = metaData.getTables(connection.getCatalog(), null, tablename, null);
		if (!rs.next()) {
			if (!autoCreateTable) {
				throw new ETLException("The target table '" + tablename + "' does not exist.");
			}
			createTable();
		}
		rs.close();
		return metaData;
	}
	
	/**
	 * Checks table structure and creates missing columns if autoCreateColumns is enabled
	 * @throws ETLException 
	 * @throws SQLException 
	 * 
	 */
	protected void checkTableStructure() throws ETLException, SQLException {
		
		DatabaseMetaData metaData = checkTableExists();
		
		ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tablename, null);
		//dumpResultSet(rs);
		
		columns = new LinkedHashMap<String, DbColumnDef>();
		while (rs.next()) {
			String name = rs.getString("COLUMN_NAME");
			int type = rs.getInt("DATA_TYPE");
			int size = rs.getInt("COLUMN_SIZE");
			String allowNull = rs.getString("NULLABLE");
			DbColumnDef colDef = new DbColumnDef(name, type, size, allowNull.equals("YES") || allowNull.equals("1") || allowNull.equals("TRUE"));
			columns.put(name.toUpperCase(), colDef);
		}
		rs.close();
		
		List<IColumn> missingCols = new ArrayList<IColumn>();
		
		// Check if the columns apply.
		for (IColumn column : processContext.getDataSet().getColumns()) {
			if (!column.isExclude()) {
				DbColumnDef dbc = columns.get(column.computeTargetName().toUpperCase());
				if (dbc == null) {
					// try column name
					dbc = columns.get(column.getName().toUpperCase());
				}
				if (dbc != null) {
					dbc.setColumn(column);
				} else {
					processContext.getMonitor().logWarn("Column does not exist: " + column.computeTargetName());
					missingCols.add(column);
				}
			}
		}
		if (missingCols.size() > 0) {
			if (autoCreateColumns) {
				if (autoDetectColumnTypes) {
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
	 * Do an addition full source scan of value analysis.
	 * @param missingCols
	 * @throws ETLException 
	 */
	protected void autoDetectColumnTypes(List<IColumn> missingCols) throws ETLException {
		IETLProcess process = (IETLProcess)processContext.getProcess();
		IExtractor extractor = process.getExtractor();

		class ColumnType {
			boolean isInteger = true;
			boolean isLong = true;
			boolean isDouble = true;
			boolean isDate = true;
			int maxLength = 0;
			int count = 0; 
		}
		
		Map<IColumn, ColumnType> columnTypes = new HashMap<IColumn, ColumnType>();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/DD/yyyy");
		
		// assume source is opened already, iterate all records
		IRecord record;
		while ((record = extractor.getNextRecord()) != null) {

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
				if (value instanceof String) {
					s = (String)value;
				} else if (value instanceof Number) {
					n = (Number)value;
				} else if (value instanceof Date) {
					d = (Date)value;
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
							Integer.parseInt(s);
						} catch (Exception e) {
							columnType.isInteger = false;
						}
					}
					// check long
					if (columnType.isLong) {
						try {
							Long.parseLong(s);
						} catch (Exception e) {
							columnType.isLong = false;
						}
					}
					// check double
					if (columnType.isDouble) {
						try {
							Double.parseDouble(s);
						} catch (Exception e) {
							columnType.isDouble = false;
						}
					}
					// check date
					if (columnType.isDate) {
						try {
							dateFormat.parse(s);
						} catch (Exception e) {
							columnType.isDate = false;
						}
					}
				} else {
					if (n != null) {
						if (columnType.isInteger && !(n instanceof Integer)) {
							columnType.isInteger = false;
						}
						if (columnType.isLong && !(n instanceof Long)) {
							columnType.isLong = false;
						}
						if (columnType.isDouble && !(n instanceof Double)) {
							columnType.isDouble = false;
						}
						if (columnType.isDate && !(d instanceof Date)) {
							columnType.isDate = false;
						}
					}
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
		
		// set collected column type information
		for (Map.Entry<IColumn, ColumnType> entry : columnTypes.entrySet()) {
			IColumn column = entry.getKey();
			ColumnType columnType = entry.getValue();
			
			if (column.getTypeHint() != DataType.UNKNOWN) {
				continue;
			}
			
			boolean forceString = columnType.count == 0; 
			
			if (!forceString) {
				if (columnType.isInteger) {
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
			} else {
				columnType.maxLength = 256;
			}
			
			if (forceString) {
				// TODO support clob for length > 4000
				column.setTypeHint(DataType.STRING);
				// set length
				int lengthHint = 1;
				for (; lengthHint < columnType.maxLength; lengthHint *= 2);
				column.setLengthHint(lengthHint);
			}
		}
		
		// close source
		extractor.close();
		
		// open again
		ISource source = processContext.getCurrentSource();
		extractor.openSource(source, new DataSet());
	}

	/**
	 * Create table with primary key column Id (bigint identidy).
	 * @throws SQLException
	 */
	private void createTable() throws SQLException {
		processContext.getMonitor().logInfo("Creating missing table: " + tablename);

		Statement stmt = connection.createStatement();
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE [").append(tablename).append("] (");
		sb.append("[Id] [bigint] IDENTITY(1,1) NOT NULL, CONSTRAINT [PK_").append(tablename).append("] PRIMARY KEY (Id))");
		stmt.execute(sb.toString());
	}

	/**
	 * Truncate table
	 * @throws ETLException 
	 */
	protected void truncateTable() throws ETLException {
		try {
			Statement stmt = connection.createStatement();
			int rows;
			try {
				// try TRUNCATE TABLE
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tablename);
				rs.next();
				rows = rs.getInt(1);
				stmt.executeUpdate("TRUNCATE TABLE " + tablename);
			} catch (SQLException e) {
				// try DELETE FROM
				rows = stmt.executeUpdate("DELETE FROM " + tablename);
			}
			processContext.getMonitor().logInfo("TRUNCATED TABLE " + tablename + " - " + rows + " rows have been deleted.");
		} catch (SQLException e) {
			throw new ETLException("Error truncating table: " + e, e);
		}
	}

	/**
	 * @param missingCols
	 * @param columns 
	 * @throws SQLException 
	 */
	private void createColumns(List<IColumn> missingCols, Map<String, DbColumnDef> columns) throws SQLException {

		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE [").append(tablename).append("] ADD ");
		boolean first = true;
		
		for (IColumn col : missingCols) {
			
			// if a column is used twice, we must make sure that it is not created twice as well!
			if (!columns.containsKey(col.computeTargetName())) {
				DbColumnDef dbcd = new DbColumnDef(col.computeTargetName());
				dbcd.setColumn(col);
				
				if (first) {
					first = false;
				} else {
					sql.append(", ");
				}
				
				sql.append("[")
				   .append(col.computeTargetName())
				   .append("] ");
				
				String type;;
				switch (col.getTypeHint()) {
				case DATE:
					type = "DATETIME";
					dbcd.setType(Types.TIMESTAMP);
					break;
				case DATETIME:
					type = "DATETIME";
					dbcd.setType(Types.TIMESTAMP);
					break;
				case DOUBLE:
					type = "FLOAT";
					dbcd.setType(Types.FLOAT);
					break;
				case INT:
					type = "INT";
					dbcd.setType(Types.INTEGER);
					break;
				case LONG:
					type = "BIGINT";
					dbcd.setType(Types.BIGINT);
					break;
				default:
					int length = col.getLengthHint();
					if (length < 1) {
						length = 255;
					}
					type = "VARCHAR(" + length + ")";
					dbcd.setType(Types.VARCHAR);
					dbcd.setSize(length);
					break;
				}
				
				sql.append(type)
				   .append(" NULL");
				
				dbcd.setAllowsNull(true);
				columns.put(dbcd.getName(), dbcd);
			}
			
		}
		
		processContext.getMonitor().logInfo("Creating missing columns: \n" + sql.toString());

		Statement stmt = connection.createStatement();
		stmt.execute(sql.toString());
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {

		try {
			switch (mode) {
			case INSERT:
				doInsert(processContext, record);
				break;
			case UPDATE:
				doUpdate(processContext, record);
				break;
			case INSERT_OR_UPDATE:
				if (Validate.equals(newIdentifierValue, record.getData(newIdentifierColumn))) {
					doInsert(processContext, record);
				} else {
					doUpdate(processContext, record);
				}
			}
		} catch (Throwable t) {
			record.markInvalid(t.getLocalizedMessage());
			processContext.getMonitor().logError("Cannot process record " + processContext.getRecordsCount() , t);
			if (!skipError) {
				throw new ETLException(t);
			}
		}
		
	}

	private void doUpdate(IProcessContext processContext, IRecord record) throws ETLException {
		
		
		try {
			psUpdate.clearParameters();
			
			int idx = 1;
			boolean modified = false;
			DbColumnDef pkColDef = null;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null && !colDef.getName().equalsIgnoreCase(pkColumn)) {
					
					Object value = record.getData(colDef.getColumn());
					setPSValue(psUpdate, idx++, value, colDef);
					
					modified = modified | record.isChanged(colDef.getColumn());
					
				}

				// PK might be excluded
				if (colDef.getName().equals(pkColumn)) {
					pkColDef = colDef;
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
					batchCountUpdate++;
					if (batchCountUpdate >= batchSize) {
						// execute
						executeBatch();
					}
				}
			}

			
		} catch (DataTruncation dt) {
			monitor.logError("Data Truncation during INSERT record (fields with value lengths following): " + record, dt);
			// log field value lengths
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					Object value = record.getData(colDef.getColumn());
					if (value == null) {
						continue;
					}
					monitor.logInfo(colDef.getName() + ":(" + value.toString().length() + ")=" + value);
				}
			}
			throw new ETLException("A Data Truncation occured during INSERT.", dt);
		} catch (SQLException se) {
			monitor.logError("Error during INSERT record: " + record, se);
			throw new ETLException("A SQLException occured during INSERT: " + se, se);
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
	private void setPSValue(PreparedStatement ps, int idx, Object value, DbColumnDef colDef) throws SQLException, ETLException {

		if (value == null || (treatEmptyAsNull && value instanceof String && ((String)value).length() == 0)) {
			ps.setNull(idx, colDef.getType());
		} else {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.CLOB:
			case -9: { //Types.NVARCHAR: <-- This does not exist in any java version prior to 1.6, so I use the hardcoded value here.
				String s = value.toString();
				if (autoDataTruncate && s.length() > colDef.getSize()) {
					monitor.logWarn("Truncate value for column '" + colDef.getName() + "' from " + s.length() + " to " + colDef.getSize() + " character");
					s = s.substring(0, colDef.getSize());
				}
				ps.setString(idx, s);
				break;
			}
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.SMALLINT:
				try {
					if (value instanceof Integer) {
						ps.setInt(idx, (Integer)value);
					} else if (value instanceof Long) {
						ps.setLong(idx, (Long)value);
					} else if (value instanceof String) {
						ps.setInt(idx, Integer.parseInt((String)value));
					} else {
						// unknown value
						ps.setObject(idx, value);
					}
					break;
				} catch (NumberFormatException nfe) {
					// add column information
					throw new NumberFormatException(nfe.getMessage() + ", column '" + colDef.getName() + "'");
				}
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
				if (value instanceof Integer) {
					ps.setInt(idx, (Integer)value);
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
				if (value instanceof Date) {
					ps.setDate(idx, new java.sql.Date(((Date)value).getTime()));
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
			case Types.TINYINT:
				if (value instanceof Boolean) {
					Boolean b = (Boolean)value;
					ps.setBoolean(idx, b.booleanValue());
				} else if (value instanceof Number) {
					Number valNum = (Number) value;
					ps.setBoolean(idx, valNum.intValue() == 0 ? false : true);
				} else if (value instanceof String) {
					Integer valInt = Integer.parseInt((String) value);
					ps.setBoolean(idx, valInt == 0 ? false : true);
				} else {
					// unknown value
					ps.setObject(idx, value);
				}
				break;
			default:
				throw new ETLException("Unknown datatype: "+ colDef.getType());
			}
			
		}
		
	}

	private void doInsert(IProcessContext processContext, IRecord record) throws ETLException {
		try {
			psInsert.clearParameters();
			
			int idx = 1;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					
					Object value = record.getData(colDef.getColumn());
					setPSValue(psInsert, idx++, value, colDef);
					
					
				}
			}
			
			if (batchSize < 1) {
				// non-batched mode
				int count = psInsert.executeUpdate();
				if (count != 1) {
					monitor.logWarn("Insert resulted in count " + count + " but expected 1");
				}
				insertCount += count;
			} else {
				// batched mode
				psInsert.addBatch();
				batchCountInsert++;
				if (batchCountInsert >= batchSize) {
					// execute
					executeBatch();
				}
			}
			
		} catch (DataTruncation dt) {
			monitor.logError("Data Truncation during INSERT record (fields with value lengths following): " + record, dt);
			// log field value lengths
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					Object value = record.getData(colDef.getColumn());
					if (value == null) {
						continue;
					}
					monitor.logInfo(colDef.getName() + "{" + colDef.getSize() + "}=" + value + "{" + value.toString().length() + "}");
				}
			}
			throw new ETLException("A Data Truncation occured during INSERT.", dt);
		} catch (SQLException se) {
			monitor.logError("Error during INSERT record: " + record, se);
			throw new ETLException("A SQLException occured during INSERT: " + se, se);
		}
		
	}
	
	
	/**
	 * @throws SQLException 
	 * 
	 */
	private void executeBatch() throws SQLException {
		// batch insert
		if (batchCountInsert > 0) {
			for (int count : psInsert.executeBatch()) {
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
			int[] result = psUpdate.executeBatch();
			for (int i = 0; i < result.length; i++) {
				int count = result[i];
				if (count != 1) {
					monitor.logWarn("Update resulted in count " + count + " but expected 1");
				}	
				updateCount += count;
			}			
			batchCountUpdate = 0;
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
		this.tablename = tablename;
	}

	/**
	 * @param catalogName the catalogName to set
	 * @deprecated no longer required.
	 */
	public void setCatalogName(String catalogName) {
		// do nothing.
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
	 * Add columns that exist in the target table but are not touched. This eleminates
	 * a warning about columns that exist int he table but do not come from the source.
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

}
