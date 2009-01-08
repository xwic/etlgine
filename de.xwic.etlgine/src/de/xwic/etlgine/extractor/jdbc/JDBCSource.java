/**
 * 
 */
package de.xwic.etlgine.extractor.jdbc;

import de.xwic.etlgine.ISource;

/**
 * Defines a query to a database via JDBC. 
 * @author lippisch
 */
public class JDBCSource implements ISource {

	private String connectionName = null;
	// by default use the JTDS driver...
	private String driverName = "net.sourceforge.jtds.jdbc.Driver";
	private String connectionUrl = null;
	private String username = null;
	private String password = null;
	
	private String sqlSelectString = null;

	
	/**
	 * @param driverName
	 * @param connectionUrl
	 * @param username
	 * @param password
	 * @param sqlSelectString
	 */
	public JDBCSource(String driverName, String connectionUrl, String username,
			String password, String sqlSelectString) {
		super();
		this.driverName = driverName;
		this.connectionUrl = connectionUrl;
		this.username = username;
		this.password = password;
		this.sqlSelectString = sqlSelectString;
	}

	/**
	 * @param driverName
	 * @param connectionUrl
	 * @param username
	 * @param password
	 */
	public JDBCSource(String driverName, String connectionUrl, String username,
			String password) {
		super();
		this.driverName = driverName;
		this.connectionUrl = connectionUrl;
		this.username = username;
		this.password = password;
	}

	/**
	 * @param connectionName
	 * @param sqlSelectString
	 */
	public JDBCSource(String connectionName, String sqlSelectString) {
		super();
		this.connectionName = connectionName;
		this.sqlSelectString = sqlSelectString;
	}

	/**
	 * @param connectionName
	 */
	public JDBCSource(String connectionName) {
		super();
		this.connectionName = connectionName;
	}

	/**
	 * Default Constructor.
	 */
	public JDBCSource() {
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#getName()
	 */
	public String getName() {
		return "JDBC " + (connectionName != null ? "[" + connectionName + "]" : connectionUrl);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#isAvailable()
	 */
	public boolean isAvailable() {
		return true;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#isOptional()
	 */
	public boolean isOptional() {
		return false;
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
	 * @return the sqlSelectString
	 */
	public String getSqlSelectString() {
		return sqlSelectString;
	}

	/**
	 * @param sqlSelectString the sqlSelectString to set
	 */
	public void setSqlSelectString(String sqlSelectString) {
		this.sqlSelectString = sqlSelectString;
	}

}
