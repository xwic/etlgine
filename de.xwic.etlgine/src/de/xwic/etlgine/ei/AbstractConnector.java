/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Abstract connector implementation.
 * 
 * @author lippisch
 */
public abstract class AbstractConnector implements IConnector {

	protected final Log log = LogFactory.getLog(getClass()); 
	
	protected ConnectorConfig config;
	protected Connection connection;
	
	protected List<Statement> stmtBin = new ArrayList<Statement>();

	protected Credentials credentials;

	/* (non-Javadoc)
	 * @see com.netapp.pulse.ei.IConnector#close()
	 */
	@Override
	public void close() throws EIException {
		try {
			if (connection != null) {
				connection.close();
			}
			for (Statement stmt : stmtBin) {
				try {
					stmt.close();
				} catch (Throwable t) {
					log.error("Error closing statement: " + t);
				}
			}
		} catch (SQLException se) {
			throw new EIException("Error closing connection or statements: " + se, se);
		}
	}

	/**
	 * Close any open statements.
	 */
	public void closeStatements() {
		for (Statement stmt : stmtBin) {
			try {
				stmt.close();
			} catch (Throwable t) {
				log.error("Error closing statement: " + t);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.netapp.pulse.ei.IConnector#initialize(com.netapp.pulse.ei.ConnectorConfig)
	 */
	@Override
	public void initialize(ConnectorConfig config, Connection connection, Credentials credentials) throws EIException {
		this.config = config;
		this.connection = connection;
		this.credentials = credentials;
	}
	

	/**
	 * Escape strings for direct SQL queries.
	 * @param upperCase
	 * @return
	 */
	protected String escape(String s) {
		
		return s.replace("'", "''");
		
	}
	
	/**
	 * Prepare a statement from a SQL template file that is stored in the classpath with
	 * the connector.
	 * @param templateName
	 * @return
	 * @throws IOException 
	 * @throws SQLException 
	 */
	protected PreparedStatement prepareStatementFromTemplate(String templateName, Object... formatArgs) throws EIException {

		try {
			InputStream in = getClass().getResourceAsStream(templateName);
			if (in == null) {
				throw new IllegalStateException(templateName + " not found in classpath.");
			}
			String sql = readAll(in);
			in.close();
			
			if (connection == null) {
				throw new IllegalStateException("Connection not initialized!");
			}
			
			if (formatArgs != null && formatArgs.length != 0) {
				sql = String.format(sql, formatArgs);
			}
			
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setFetchSize(1000); // usually all data is consumed, thus a larger fetch size increases performance.
			stmtBin.add(ps); // make sure it gets closed when the connector is closed.
			return ps;
			
		} catch (SQLException e) {
			throw new EIException("Error creating prepared statement", e);
		} catch (Exception e) {
			throw new EIException("Can not read template '" + templateName + "'");
		}

	}
	
	/**
	 * Executes a query based on the specified template name as a prepared statement with the
	 * specified arguments. Returns the value of the first row in the first column or NULL if no 
	 * value was found.
	 * Usefull for simple queries that return a single result such as SELECT username FROM xyz WHERE id=?
	 * @param templateName
	 * @param arguments
	 * @return
	 * @throws EIException
	 */
	protected Object executeSingleQueryFromTemplate(String templateName, Object... arguments) throws EIException {
		
		try {
			ResultSet rs = executeFromTemplate(templateName, arguments);
			if (rs.next()) {
				Object obj = rs.getObject(1);
				if (rs.wasNull()) {
					obj = null;
				}
				rs.getStatement().close();
				rs.close();
				return obj;
			}
			rs.close();
			return null;
		} catch (SQLException se) {
			throw new EIException("Error executing query:" + se, se);
		}
		
	}
	
	/**
	 * Returns the result of a single query form a template as Integer.
	 * @param templateName
	 * @param arguments
	 * @return
	 * @throws EIException
	 */
	protected Integer executeSingleQueryFromTemplateAsInteger(String templateName, Object... arguments) throws EIException {
		Object obj = executeSingleQueryFromTemplate(templateName, arguments);
		if (obj != null) {
			if (obj instanceof Integer) {
				return (Integer)obj;
			} else if (obj instanceof BigInteger) {
				return ((BigInteger)obj).intValue();
			} else if (obj instanceof BigDecimal) {
				return ((BigDecimal)obj).intValue();
			} else if (obj instanceof Long) {
				return ((Long)obj).intValue();
			} else if (obj instanceof String) {
				return Integer.parseInt((String)obj);
			}
			throw new EIException("Expected numeric/int value but query returned " + obj.getClass().getName());
		}
		return null;
	}

	/**
	 * Returns the result of a single query form a template as String.
	 * @param templateName
	 * @param arguments
	 * @return
	 * @throws EIException
	 */
	protected String executeSingleQueryFromTemplateAsString(String templateName, Object... arguments) throws EIException {
		Object obj = executeSingleQueryFromTemplate(templateName, arguments);
		if (obj != null) {
			return obj.toString();
		}
		return null;
	}

	/**
	 * @param string
	 * @param name
	 * @return
	 * @throws EIException, SQLException 
	 */
	protected ResultSet executeFromTemplate(String templateName, Object... arguments) throws EIException, SQLException {
		
		PreparedStatement ps = prepareStatementFromTemplate(templateName);
		int idx = 1;
		for (Object o : arguments) {
			if (o instanceof String) {
				ps.setString(idx, (String)o);
			} else if (o instanceof Integer) {
				ps.setInt(idx, (Integer)o);
			} else if (o instanceof java.sql.Date) {
				ps.setDate(idx, (java.sql.Date)o);
			} else if (o instanceof java.util.Date) {
				ps.setDate(idx, new java.sql.Date(((java.util.Date)o).getTime()));
			}
			
			idx++;
		}
		return ps.executeQuery();
	}

	
	/**
	 * Read the content from the InputStream into a string.
	 * @param in
	 * @return
	 * @throws IOException
	 */
	protected String readAll(InputStream in) throws IOException {
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String s;
		StringBuilder sb = new StringBuilder();
		while ((s = reader.readLine()) != null) {
			sb.append(s).append("\n");
		}
		reader.close();
		return sb.toString();
		
	}
				
		
}
