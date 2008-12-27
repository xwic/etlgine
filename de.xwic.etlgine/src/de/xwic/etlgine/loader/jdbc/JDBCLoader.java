/**
 * 
 */
package de.xwic.etlgine.loader.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 *
 */
public class JDBCLoader extends AbstractLoader {

	// by default use the JTDS driver...
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl = null;
	private String username = null;
	private String password = null;
	private String catalogName = null;
	private String tablename = null;
	private boolean autoCreateColumns = false;
	
	private Connection connection = null;
	private PreparedStatement psInsert = null;
	private Map<String, DbColumnDef> columns;
	private Set<String> ignoredColumns = new HashSet<String>();
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		// initialize the driver
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			throw new ETLException("The specified driver (" + driverName + ") can not be found.", e);
		}
		
		if (connectionUrl == null) {
			throw new ETLException("No connection URL specified");
		}
		if (username == null) {
			throw new ETLException("No username specified");
		}
		if (password == null) {
			throw new ETLException("No password specified");
		}
		
		try {
			connection = DriverManager.getConnection(connectionUrl, username, password);
		} catch (SQLException e) {
			throw new ETLException("Error opening connect: " + e, e);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractLoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
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
			ResultSet rs = metaData.getTables(catalogName, null, tablename, null);
			if (!rs.next()) {
				throw new ETLException("The target table '" + tablename + "' does not exist.");
			}
			
			rs = metaData.getColumns(catalogName, null, tablename, null);
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
					throw new ETLException("The source contains columns that do not exist in the target table.");
				}
			}
			
			// build prepared statement..
			StringBuilder sql = new StringBuilder();
			StringBuilder sqlValues = new StringBuilder();
			sql.append("INSERT INTO [").append(tablename).append("] (");
			sqlValues.append("(");
			
			boolean first = true;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					if (first) {
						first = false;
					} else {
						sql.append(", ");
						sqlValues.append(", ");
					}
					sql.append("[");
					sql.append(colDef.getName());
					sql.append("]");
					sqlValues.append("?");
				} else {
					if (!ignoredColumns.contains(colDef.getName())) {
						monitor.logWarn("A column in the target table does not exist in the source and is skipped (" + colDef.getName() + ")");
					}
				}
			}
			sqlValues.append(")");
			sql.append(") VALUES").append(sqlValues);
			
			monitor.logInfo("INSERT Statement: " + sql);
			
			psInsert = connection.prepareStatement(sql.toString());
			
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
		
		processContext.getMonitor().logInfo("Creating missing columns: \n" + sql.toString());

		Statement stmt = connection.createStatement();
		stmt.execute(sql.toString());
		
	}

	/**
	 * @param rs
	 * @throws SQLException 
	 */
	protected void dumpResultSet(ResultSet rs) throws SQLException {
		
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 0; i < rsmd.getColumnCount(); i++) {
			System.out.print(rsmd.getColumnName(i + 1));
			System.out.print(" ");
		}
		System.out.println("");
		while (rs.next()) {
			for (int i = 0; i < rsmd.getColumnCount(); i++) {
				System.out.print(rs.getString(i + 1));
				System.out.print(" ");
			}
			System.out.println();
		}

		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {

		try {
			psInsert.clearParameters();
			
			int idx = 1;
			for (DbColumnDef colDef : columns.values()) {
				if (colDef.getColumn() != null) {
					
					Object value = record.getData(colDef.getColumn());
					if (value == null) {
						psInsert.setNull(idx, colDef.getType());
					} else {
						switch (colDef.getType()) {
						case Types.VARCHAR:
						case Types.CHAR:
						case Types.LONGVARCHAR:
						case Types.NVARCHAR:
							psInsert.setString(idx, value.toString());
							break;
						case Types.INTEGER:
						case Types.BIGINT:
							if (value instanceof Integer) {
								psInsert.setInt(idx, (Integer)value);
							} else if (value instanceof String) {
								psInsert.setInt(idx, Integer.parseInt((String)value));
							}
							break;
						case Types.DOUBLE:
						case Types.FLOAT:
							if (value instanceof Integer) {
								psInsert.setInt(idx, (Integer)value);
							} else if (value instanceof String) {
								psInsert.setDouble(idx, Double.parseDouble((String)value));
							} else if (value instanceof Double) {
								psInsert.setDouble(idx, (Double)value);
							}
							break;							
						case Types.TIMESTAMP:
						case Types.DATE:
							psInsert.setDate(idx, new java.sql.Date(((Date)value).getTime()));
							break;
						default:
							throw new ETLException("Unknown datatype: "+ colDef.getType());
						}
						
					}
					idx++;
				}
			}
			
			int count = psInsert.executeUpdate();
			if (count != 1) {
				monitor.logWarn("Insert resulted in count " + count + " but expected 1");
			}
		} catch (SQLException se) {
			throw new ETLException("An SQLException occured during INSERT: " + se, se);
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
	 * @return the catalogName
	 */
	public String getCatalogName() {
		return catalogName;
	}

	/**
	 * @param catalogName the catalogName to set
	 */
	public void setCatalogName(String catalogName) {
		this.catalogName = catalogName;
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
	
}
