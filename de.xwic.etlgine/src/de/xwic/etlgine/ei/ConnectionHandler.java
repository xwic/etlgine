/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages connections.
 * @author Lippisch
 */
public class ConnectionHandler {

	private final Log log = LogFactory.getLog(ConnectionHandler.class);
	
	private BasicDataSource ds;
	private Set<TracedConnection> connectionList = Collections.synchronizedSet(new HashSet<TracedConnection>());
	
	private long connCount = 0;

	private final ConnectionConfig conConfig;

	public ConnectionHandler(ConnectionConfig conConfig) {
		this.conConfig = conConfig;
		
		if (conConfig.isPooled()) {
			initializeConnectionPool();
		} else {
			try {
				Class.forName(conConfig.getDriver());
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException("The configured driver '" + conConfig.getDriver() + "' can not be found.");
			}
		}

	}
	
	private void initializeConnectionPool() {
		try {
			log.info("Initializing connection " + conConfig.getId());

			// DBCP properties used to create the BasicDataSource
			Properties dbcpProperties = new Properties();

			// DriverClass & url
			dbcpProperties.put("driverClassName", conConfig.getDriver());
			dbcpProperties.put("url", conConfig.getUrl());

			// Username / password
			dbcpProperties.put("username", conConfig.getUsername());
			dbcpProperties.put("password", conConfig.getPassword());

			// Pool size
			dbcpProperties.put("maxActive", Integer.toString(conConfig.getPoolSize()));

			// Let the factory create the pool
			ds = (BasicDataSource) BasicDataSourceFactory.createDataSource(dbcpProperties);

			ds.setMaxWait(conConfig.getMaxWait()); // wait max before throwing an exception.
			ds.setTestOnBorrow(true);
			ds.setValidationQuery(conConfig.getValidationQuery());

			if (!conConfig.isLazyInit()) {
				// The BasicDataSource has lazy initialization
				// borrowing a connection will start the DataSource
				// and make sure it is configured correctly.
				Connection conn = ds.getConnection();
				conn.close();
			}

		} catch (Throwable e) {
			String message = "Unable to create DHCP pool... ";
			log.fatal(message, e);
			if (ds != null) {
				try {
					ds.close();
				} catch (Exception e2) {
					// ignore
				}
				ds = null;
			}
		}
		log.info("Configure ConnectionHandler '" + conConfig.getId() + "' complete");
	}

	/**
	 * Borrow a new connection.
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		connCount++;
		if (connCount == Long.MAX_VALUE) {
			connCount = Long.MIN_VALUE;
		}
		Connection conn = null;
		if (conConfig.isPooled()) {
			conn = ds.getConnection();
		} else {
			// Open connection directly
			conn = DriverManager.getConnection(conConfig.getUrl(), conConfig.getUsername(), conConfig.getPassword());
		}
		if (conConfig.isTraced()) {
			TracedConnection tc = new TracedConnection(connCount, this, conn);
			conn = tc;
			connectionList.add(tc);
		}
		return conn;
	}
	
	/**
	 * Remove traced connection from tracking list.
	 * @param tc
	 */
	void connectionClosed(TracedConnection tc) {
		connectionList.remove(tc);
	}

	/**
	 * Close the data source.
	 * @throws HibernateException
	 */
	public void destroy() throws EIException {
		log.info("Close ConnectionHandler '" + conConfig.getId() + "'");
		
		if (connectionList.size() > 0) {
			log.warn("There are still " + connectionList.size() + " connections open..");
		}
		
		try {
			if (ds != null) {
				ds.close();
				ds = null;
			} else {
				log.warn("Cannot close DBCP pool (not initialized)");
			}
		} catch (Exception e) {
			throw new EIException("Could not close DBCP pool", e);
		}
		log.info("Close DBCPConnectionProvider complete");
	}

	/**
	 * @return the connectionList
	 */
	public Set<TracedConnection> getConnectionList() {
		return connectionList;
	}
	
	/**
	 * Return number of active connections.
	 * @return
	 */
	public int getNumActive() {
		if (ds == null) {
			return 0;
		}
		return ds.getNumActive();
	}

	/**
	 * Return number of active connections.
	 * @return
	 */
	public int getNumIdle() {
		if (ds == null) {
			return 0;
		}
		return ds.getNumIdle();
	}

	/**
	 * Return number of active connections.
	 * @return
	 */
	public int getMaxActive() {
		if (ds == null) {
			return 0;
		}
		return ds.getMaxActive();
	}

	/**
	 * Returns the id of the connection managed.
	 * @return
	 */
	public String getConnectionId() {
		return conConfig.getId();
	}
	
}
