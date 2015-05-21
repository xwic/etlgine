/**
 * 
 */
package de.xwic.etlgine.extractor.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import de.xwic.etlgine.AbstractExtractor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * @author lippisch
 */
public class JDBCExtractor extends AbstractExtractor {

	private static Logger log = Logger.getLogger(JDBCExtractor.class.getName());
	
	private Statement stmt = null;
	private ResultSet rs = null;
	private Connection connection = null;
	private JDBCSource currSource = null;
	private boolean logSqlSelectString = true;
	
	private boolean endReached = false;
	private int colCount = 0;
	
	private int fetchSize = -1;
	private int returnedCount = 0;
	private int getNextRecordInvoked = 0;
	
	private int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE; // on MSSQL using jTDS ResultSet.TYPE_FORWARD_ONLY should be used TODO check changing default to ResultSet.TYPE_FORWARD_ONLY
	private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
	
	private List<DataType> typeHints = new ArrayList<DataType>();
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractExtractor#initialize(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#close()
	 */
	public void close() throws ETLException {
		if (rs != null) {
			try {
				Statement stmt = rs.getStatement();
				if (stmt != null) {
					stmt.close();
				}
				rs.close();
			} catch (Throwable t) {
				context.getMonitor().logError("Error closing ResultSet", t);
				// continue -> try to close the connection..
			} finally {
				rs = null;
			}
		}
		if (connection != null && currSource.getSharedConnectionName() == null) {
			try {
				connection.close();
			} catch (Throwable t) {
				throw new ETLException("Error closing Connection", t);
			}
		}
		connection = null;

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {

		getNextRecordInvoked++;
		
		if (endReached) {
			return null;
		}
		int i = 1;
		IColumn col = null;
		try {
			if (rs.next()) {
				IRecord record = context.newRecord();
				IDataSet ds = context.getDataSet();
				for (; i <= colCount; i++) {
					col = ds.getColumnByIndex(i);
					DataType typeHint = typeHints.get(i - 1);
					Object value = null;
					switch (typeHint) {
					case STRING: 
						value = rs.getString(i);
						break;
					case INT:
						value = rs.getInt(i);
						break;
					case LONG:
						value = rs.getLong(i);
						break;
					case BIGDECIMAL:
						value = rs.getBigDecimal(i);
						break;
					case DATE:
					case DATETIME:
						if (currSource.isUseJavaDate()) {
							Timestamp ts = rs.getTimestamp(i);
							if (ts != null) {
								value = new Date(ts.getTime());
							}
						} else {
							value = rs.getDate(i); // loses the time in jTDS, use useJavaDate instead (sql.TimeStamp could be used as well)
						}
						break;
					case DOUBLE:
						value = rs.getDouble(i);
						break;
					default:
						value = rs.getObject(i);
						break;
					}
					if (rs.wasNull()) {
						value = null;
					}
					record.setData(col, value);
				}
		
				record.resetChangeFlag();
				returnedCount++;
				return record;
			} else {
				endReached = true;
			}
		} catch (SQLException se) {
			throw new ETLException("Error reading resultSet at column '" + col.getName() + "': " + se, se);
		}
		
		
		return null;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {
		
		if (!(source instanceof JDBCSource)) {
			throw new ETLException("Invalid Source type - JDBCSource expected.");
		}
		currSource = (JDBCSource) source;
		if (currSource.getSqlSelectString() == null) {
			throw new ETLException("No SQL SELECT specified!");
		}
		
		if (currSource.getConnectionName() == null) {
			if (currSource.getConnectionUrl() == null) {
				throw new ETLException("No connection NAME or URL specified");
			}
			if (currSource.getUsername() == null) {
				throw new ETLException("No username specified");
			}
			if (currSource.getPassword() == null) {
				throw new ETLException("No password specified");
			}
			try {
				log.info("Using direct connection - URL: " + currSource.getConnectionUrl());
				// initialize the driver
				try {
					Class.forName(currSource.getDriverName());
				} catch (ClassNotFoundException e) {
					throw new ETLException("The specified driver (" + currSource.getDriverName() + ") can not be found.", e);
				}
				
				connection = DriverManager.getConnection(currSource.getConnectionUrl(), currSource.getUsername(), currSource.getPassword());
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		} else {
			log.info("Using named connection: " + currSource.getConnectionName());
			try {
				if (currSource.getSharedConnectionName() != null) {
					connection = JDBCUtil.getSharedConnection(context, currSource.getSharedConnectionName(), currSource.getConnectionName());
				} else {
					connection = JDBCUtil.openConnection(context, currSource.getConnectionName());
				}
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		}

		try {
			//stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			stmt = connection.createStatement(resultSetType, resultSetConcurrency);

			// set fetch size
			if (fetchSize == -1) {
				fetchSize = JDBCUtil.getFetchSize(context, currSource.getConnectionName());
			}
			
			if (fetchSize > 0) {
				stmt.setFetchSize(fetchSize);
			}
			String sql = currSource.getSqlSelectString();
			if (isLogSqlSelectString()) {
				log.debug(sql);
			}
			rs = stmt.executeQuery(sql);
			
			ResultSetMetaData metaData = rs.getMetaData();
			colCount = metaData.getColumnCount();
			typeHints.clear();
			for (int i = 1; i <= colCount; i++) {
				String name = metaData.getColumnLabel(i);
				IColumn column = null;
				if (!dataSet.containsColumn(name)) {
					column = dataSet.addColumn(name, i);
				} else {
					column = dataSet.getColumn(name);
					column.setSourceIndex(i);
					// changed source index update
					dataSet.updateColumn(column);
				}
				IColumn.DataType dt = column.getTypeHint();
				int lengthHint = metaData.getPrecision(i);
				int scale = metaData.getScale(i);
				int type = metaData.getColumnType(i);
				boolean signed = metaData.isSigned(i);
				switch (type) {
				case Types.CHAR:
				case Types.VARCHAR:
				case -15: //Types.NCHAR:
				case -9: //Types.NVARCHAR:
				case Types.CLOB:
					dt = DataType.STRING;
					break;
				case Types.DECIMAL:
				case Types.NUMERIC: // Oracle NUMBER is handled here
					if (scale > 0 || (scale == -127 && connection.getMetaData().getURL().contains("oracle"))) {
					//if (scale > 0) {
						dt = DataType.DOUBLE;
					} else if (lengthHint > 10) {
						dt = DataType.LONG;
					} else {
						dt = DataType.INT;
					}
					break;
				case Types.TINYINT:
				case Types.INTEGER:
					dt = DataType.INT;
					break;
				case Types.BIGINT:
					if (signed){
						dt = DataType.LONG;
					}else{
						dt = DataType.BIGDECIMAL;
					}
					break;
				case Types.FLOAT:
				case Types.DOUBLE:
					dt = DataType.DOUBLE;
					break;
				case Types.TIMESTAMP:
					dt = DataType.DATETIME;
					break;
				case Types.DATE:
					dt = DataType.DATE;
					break;
				case Types.BIT:
					dt = DataType.BOOLEAN;
					break;
				default:
					context.getMonitor().logWarn("Unknown SQL Type " + type + " on column " + name);
					break;
				}
				column.setLengthHint(lengthHint);

				column.setTypeHint(dt);
				
				typeHints.add(dt);				
			}
			
			endReached = false;
			
			if (sql.contains("/* break */")) {
				// break point helper, please ignore
				toString();
			}
			
		} catch (SQLException se) {
			throw new ETLException("Error executing SQL SELECT statement " + currSource.getSqlSelectString() + ": " + se, se);
		}
		

	}

	/**
	 * @return the fetchSize
	 */
	public int getFetchSize() {
		return fetchSize;
	}

	/**
	 * @param fetchSize the fetchSize to set
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * @return the resultSetType
	 */
	public int getResultSetType() {
		return resultSetType;
	}

	/**
	 * @param resultSetType the resultSetType to set
	 */
	public void setResultSetType(int resultSetType) {
		this.resultSetType = resultSetType;
	}

	/**
	 * @return the resultSetConcurrency
	 */
	public int getResultSetConcurrency() {
		return resultSetConcurrency;
	}

	/**
	 * @param resultSetConcurrency the resultSetConcurrency to set
	 */
	public void setResultSetConcurrency(int resultSetConcurrency) {
		this.resultSetConcurrency = resultSetConcurrency;
	}

	/**
	 * @return the logSqlSelectString
	 */
	public boolean isLogSqlSelectString() {
		return logSqlSelectString;
	}

	/**
	 * @param logSqlSelectString the logSqlSelectString to set
	 */
	public void setLogSqlSelectString(boolean logSqlSelectString) {
		this.logSqlSelectString = logSqlSelectString;
	}
	
}
