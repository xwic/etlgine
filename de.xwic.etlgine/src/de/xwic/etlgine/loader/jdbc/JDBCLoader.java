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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mortbay.log.Log;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.jdbc.DbColumnDef;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.util.Validate;

/**
 * @author lippisch
 *
 */
public class JDBCLoader extends AbstractLoader {

	public enum Mode {
		INSERT,
		UPDATE,
		INSERT_OR_UPDATE
	}
	
	private String connectionName = null;
	// by default use the JTDS driver...
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl = null;
	private String username = null;
	private String password = null;
	private String tablename = null;
	private boolean autoCreateColumns = false;
	private boolean ignoreMissingTargetColumns = false;
	private boolean truncateTable = false;
	private boolean autoDataTruncate = false;
	
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
				Log.info("Using direct connection - URL: " + connectionUrl);
				// initialize the driver
				try {
					Class.forName(driverName);
				} catch (ClassNotFoundException e) {
					throw new ETLException("The specified driver (" + driverName + ") can not be found.", e);
				}
				
				connection = DriverManager.getConnection(connectionUrl, username, password);
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		} else {
			Log.info("Using named connection: " + connectionName);
			try {
				connection = JDBCUtil.openConnection(processContext, connectionName);
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		}
		
		if (truncateTable) {
			try {
				Statement stmt = connection.createStatement();
				int rows = stmt.executeUpdate("DELETE FROM " + tablename);
				processContext.getMonitor().logInfo("TRUNCATED TABLE " + tablename + " - " + rows + " rows have been deleted.");
			} catch (SQLException e) {
				throw new ETLException("Error truncating table: " + e, e);
			}
		}
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
		
		monitor.logInfo("JDBCLoader " + insertCount + " records inserted, " + updateCount + " records updated.");
		
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new ETLException("Error closing connection: " + e, e);
			}
			connection = null;
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
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet rs = metaData.getTables(connection.getCatalog(), null, tablename, null);
			if (!rs.next()) {
				throw new ETLException("The target table '" + tablename + "' does not exist.");
			}
			
			rs = metaData.getColumns(connection.getCatalog(), null, tablename, null);
			//dumpResultSet(rs);
			
			columns = new LinkedHashMap<String, DbColumnDef>();
			while (rs.next()) {
				String name = rs.getString("COLUMN_NAME");
				int type = rs.getInt("DATA_TYPE");
				int size = rs.getInt("COLUMN_SIZE");
				String allowNull = rs.getString("NULLABLE");
				DbColumnDef colDef = new DbColumnDef(name, type, size, allowNull.equals("YES") || allowNull.equals("1") || allowNull.equals("TRUE"));
				columns.put(name, colDef);
			}
			
			List<IColumn> missingCols = new ArrayList<IColumn>();
			
			// Check if the columns apply.
			for (IColumn column : processContext.getDataSet().getColumns()) {
				if (!column.isExclude()) {
					DbColumnDef dbc = columns.get(column.computeTargetName());
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
					createColumns(missingCols, columns);
				} else {
					if (!ignoreMissingTargetColumns) {
						throw new ETLException("The source contains columns that do not exist in the target table.");
					}
				}
			}
			
			// build prepared statement.. and Update statement.
			StringBuilder sql = new StringBuilder();
			StringBuilder sqlValues = new StringBuilder();
			StringBuilder sqlUpd = new StringBuilder();
			sql.append("INSERT INTO [").append(tablename).append("] (");
			sqlUpd.append("UPDATE [").append(tablename).append("] SET ");
			sqlValues.append("(");
			
			boolean first = true;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					if (first) {
						first = false;
					} else {
						sql.append(", ");
						sqlValues.append(", ");
						sqlUpd.append(", ");
					}
					sql.append("[");
					sql.append(colDef.getName());
					sql.append("]");
					sqlValues.append("?");

					sqlUpd.append("[");
					sqlUpd.append(colDef.getName());
					sqlUpd.append("] = ?");
				} else {
					if (!ignoredColumns.contains(colDef.getName())) {
						monitor.logWarn("A column in the target table does not exist in the source and is skipped (" + colDef.getName() + ")");
					}
				}
			}
			sqlValues.append(")");
			sql.append(") VALUES").append(sqlValues);
			
			sqlUpd.append(" WHERE [" + pkColumn + "] = ?");
			
			monitor.logInfo("INSERT Statement: " + sql);
			psInsert = connection.prepareStatement(sql.toString());

			if (mode == Mode.UPDATE || mode == Mode.INSERT_OR_UPDATE) {
				monitor.logInfo("UPDATE Statement: " + sqlUpd);
				psUpdate = connection.prepareStatement(sqlUpd.toString());
			}
			
			
		} catch (SQLException se) {
			throw new ETLException("Error initializing target database/tables: " + se, se);
		}
	}
	
	/**
	 * @param missingCols
	 * @param columns 
	 * @throws SQLException 
	 */
	private void createColumns(List<IColumn> missingCols, Map<String, DbColumnDef> columns) throws SQLException {

		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE [" + tablename + "] ADD ");
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
					type = "VARCHAR(255)";
					dbcd.setType(Types.VARCHAR);
					dbcd.setSize(255);
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
		
		
	}

	private void doUpdate(IProcessContext processContext, IRecord record) throws ETLException {
		
		
		try {
			psUpdate.clearParameters();
			
			int idx = 1;
			boolean modified = false;
			DbColumnDef pkColDef = null;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					
					Object value = record.getData(colDef.getColumn());
					setPSValue(psUpdate, idx++, value, colDef);
					
					modified = modified | record.isChanged(colDef.getColumn());
					
					if (colDef.getName().equals(pkColumn)) {
						pkColDef = colDef;
					}
					
				}
			}

			if (pkColDef == null) {
				throw new ETLException("The specified PK Column does not exist.");
			}
			setPSValue(psUpdate, idx, record.getData(pkColumn), pkColDef);
			
			if (!ignoreUnchangedRecords || modified) {

				int count = psUpdate.executeUpdate();
				if (count != 1) {
					monitor.logWarn("Update resulted in count " + count + " but expected 1");
				}	
				updateCount += count;
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

		if (value == null) {
			ps.setNull(idx, colDef.getType());
		} else {
			switch (colDef.getType()) {
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGVARCHAR:
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
				if (value instanceof Integer) {
					ps.setInt(idx, (Integer)value);
				} else if (value instanceof String) {
					ps.setInt(idx, Integer.parseInt((String)value));
				}
				break;
			case Types.DOUBLE:
			case Types.FLOAT:
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
				}
				break;							
			case Types.TIMESTAMP:
			case Types.DATE:
				if (value instanceof Date) {
					ps.setDate(idx, new java.sql.Date(((Date)value).getTime()));
				} else if (value instanceof String) {
					// let database worry about the format for now
					// TODO parse String to Date
					ps.setString(idx, (String)value);
				}
				break;
			case Types.TINYINT:
				if (value instanceof Integer) {
					Integer valInt = (Integer) value;
					ps.setBoolean(idx, valInt == 1 ? true : false);
				} else if (value instanceof String) {
					Integer valInt = Integer.parseInt((String) value);
					ps.setBoolean(idx, valInt == 1 ? true : false);
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
			
			int count = psInsert.executeUpdate();
			if (count != 1) {
				monitor.logWarn("Insert resulted in count " + count + " but expected 1");
			}
			
			insertCount += count;
			
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
}
