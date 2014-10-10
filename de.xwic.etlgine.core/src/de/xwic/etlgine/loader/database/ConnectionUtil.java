package de.xwic.etlgine.loader.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * 
 * @author mbogdan
 *
 */
public class ConnectionUtil {

	private final static Log LOGGER = LogFactory.getLog(ConnectionUtil.class);

	private ConnectionUtil() {
		// Utility class
	}

	/**
	 * Builds a dataSource from a connection configuration pattern.
	 * 
	 * A bit weird naming, because connections are usually obtained from dataSources, but to keep backwards compatibility we stick to this
	 * naming.
	 * 
	 * @param connectionName
	 * @param context
	 * @return
	 */
	public static DataSource initDataSource(final String connectionName, final IContext context) throws ETLException {
		String driverClassName = context.getProperty(connectionName + ".connection.driver", "net.sourceforge.jtds.jdbc.Driver");
		String url = context.getProperty(connectionName + ".connection.url");
		String username = context.getProperty(connectionName + ".connection.username");
		String password = context.getProperty(connectionName + ".connection.password");

		if (url == null) {
			throw new ETLException("The URL is not specified for the connectionName: '" + connectionName + "'");
		}
		if (username == null) {
			throw new ETLException("The username is not specified for the connectionName: '" + connectionName + "'");
		}
		if (password == null) {
			throw new ETLException("The password is not specified for the connectionName: '" + connectionName + "'");
		}

		BasicDataSource basicDataSource = new BasicDataSource();
		basicDataSource.setDriverClassName(driverClassName);
		basicDataSource.setUrl(url);
		basicDataSource.setUsername(username);
		basicDataSource.setPassword(password);
		// basicDataSource.setMaxActive(100);
		// basicDataSource.setMaxIdle(30);
		// basicDataSource.setMaxWait(10000);

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("Built a new dataSource: [driverClassName=" + basicDataSource.getDriverClassName() + ", url="
					+ basicDataSource.getUrl() + ", username=" + basicDataSource.getUsername() + "]");
		}

		return basicDataSource;
	}

	/**
	 * Tries to get a shared connection, then a pre-configured connection, then builds a connection if none of the previous were found by
	 * the given names.
	 * 
	 * @param processContext
	 * @param monitor
	 * @param connectionName
	 * @param sharedConnectionName
	 * @param driverName
	 * @param connectionUrl
	 * @param username
	 * @param password
	 * @return
	 * @throws ETLException
	 */
	public static Connection getConnection(final IProcessContext processContext, final IMonitor monitor, final String sharedConnectionName,
			final String connectionName, final String driverName, final String connectionUrl, final String username, final String password)
			throws ETLException {
		Connection connection = null;

		if (sharedConnectionName != null) {
			// First try to get the shared connection
			connection = getSharedConnection(processContext, monitor, sharedConnectionName, connectionName);
		} else if (connectionName != null) {
			// Second try to get a pre-configured connection
			connection = getPreconfiguredConnection(processContext, monitor, connectionName);
		} else {
			// If both connectionName and sharedConnectionName are null, try to build a new connection from properties
			monitor.logInfo("'connectionName' and 'sharedConnectionName' are null, trying to get new connection...");
			connection = buildConnection(monitor, driverName, connectionUrl, username, password);
		}

		return connection;
	}

	private static Connection buildConnection(final IMonitor monitor, final String driverName, final String connectionUrl,
			final String username, final String password) throws ETLException {
		Connection connection = null;

		// Validate parameters
		if (connectionUrl == null) {
			throw new ETLException("'connectionUrl' was not specified, when trying to build a new connection.");
		}
		if (username == null) {
			throw new ETLException("'username' was not specified, when trying to build a new connection.");
		}
		if (password == null) {
			throw new ETLException("'password' was not specified, when trying to build a new connection.");
		}
		if (driverName == null) {
			throw new ETLException("'driverName' was not specified, when trying to build a new connection.");
		}
		try {
			// Initialize the driver
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				throw new ETLException("The specified driver (" + driverName + ") can not be found.", e);
			}

			Properties properties = new Properties();
			properties.setProperty("user", username);
			properties.setProperty("password", password);

			monitor.logInfo("Requesting connection from the driver, for the connectionUrl: " + connectionUrl);

			connection = DriverManager.getConnection(connectionUrl, properties);

			monitor.logInfo("Connection for the connectionUrl: " + connectionUrl + " has been established.");
		} catch (SQLException e) {
			throw new ETLException("Error opening connection: " + e, e);
		}
		return connection;
	}

	private static Connection getSharedConnection(final IProcessContext processContext, final IMonitor monitor,
			final String sharedConnectionName, final String connectionName) throws ETLException {
		Connection connection = null;

		monitor.logInfo("Opening pre-configured connection for 'sharedConnectionName': " + sharedConnectionName);
		try {
			connection = JDBCUtil.getSharedConnection(processContext, sharedConnectionName, connectionName);
		} catch (SQLException e) {
			throw new ETLException("Error opening connection for 'sharedConnectionName'= " + sharedConnectionName, e);
		}
		return connection;
	}

	private static Connection getPreconfiguredConnection(final IProcessContext processContext, final IMonitor monitor,
			final String connectionName) throws ETLException {
		Connection connection = null;

		monitor.logInfo("Opening pre-configured connection for 'connectionName': " + connectionName);
		try {
			connection = JDBCUtil.openConnection(processContext, connectionName);
		} catch (SQLException e) {
			throw new ETLException("Error opening connection for 'connectionName': " + connectionName, e);
		}
		return connection;
	}

}
