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
import java.sql.Types;

import org.mortbay.log.Log;

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

	private Statement stmt = null;
	private ResultSet rs = null;
	private Connection connection = null;
	private JDBCSource currSource = null;
	
	private boolean endReached = false;
	private int colCount = 0;
	
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
				rs.close();
			} catch (SQLException se) {
				context.getMonitor().logError("Error closing ResultSet", se);
				// continue -> try to close the connection..
			}
		}
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
	 * @see de.xwic.etlgine.IExtractor#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {
		
		if (endReached) {
			return null;
		}
		try {
			if (rs.next()) {
				IRecord record = context.newRecord();
				IDataSet ds = context.getDataSet();
				for (int i = 1; i <= colCount; i++) {
					IColumn col = ds.getColumnByIndex(i);
					Object value = null;
					switch (col.getTypeHint()) {
					case STRING: 
						value = rs.getString(i);
						break;
					case INT:
						value = rs.getInt(i);
						break;
					case LONG:
						value = rs.getLong(i);
						break;
					case DATE:
					case DATETIME:
						value = rs.getDate(i);
						break;
					case DOUBLE:
						value = rs.getDouble(i);
						break;
					default:
						value = rs.getObject(i);
						break;
					}
					record.setData(col, value);
				}
				
				return record;
			} else {
				endReached = true;
			}
		} catch (SQLException se) {
			throw new ETLException("Error reading resultSet: " + se, se);
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
				Log.info("Using direct connection - URL: " + currSource.getConnectionUrl());
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
			Log.info("Using named connection: " + currSource.getConnectionName());
			try {
				connection = JDBCUtil.openConnection(context, currSource.getConnectionName());
			} catch (SQLException e) {
				throw new ETLException("Error opening connect: " + e, e);
			}
		}

		try {
			stmt = connection.createStatement();
			rs = stmt.executeQuery(currSource.getSqlSelectString());
			
			ResultSetMetaData metaData = rs.getMetaData();
			colCount = metaData.getColumnCount();
			for (int i = 1; i <= colCount; i++) {
				String name = metaData.getColumnLabel(i);
				if (!dataSet.containsColumn(name)) {
					IColumn column = dataSet.addColumn(name, i);
					IColumn.DataType dt = column.getTypeHint();
					switch (metaData.getColumnType(i)) {
					case Types.CHAR:
					case Types.VARCHAR:
						dt = DataType.STRING;
						break;
					case Types.INTEGER:
						dt = DataType.INT;
						break;
					case Types.BIGINT:
						dt = DataType.LONG;
						break;
					case Types.FLOAT:
						dt = DataType.DOUBLE;
						break;
					case Types.TIMESTAMP:
						dt = DataType.DATETIME;
						break;
					case Types.DATE:
						dt = DataType.DATE;
						break;
					}
					column.setTypeHint(dt);
				}
			}
			
			endReached = false;
		} catch (SQLException se) {
			throw new ETLException("Error executing SQL SELECT statement " + currSource.getSqlSelectString() + ": " + se, se);
		}
		

	}

}
