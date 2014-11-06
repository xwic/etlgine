package de.xwic.etlgine.loader.database;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;

/**
 * Utility class that builds DB dataSources, and stored them in the context.
 *
 * @author mbogdan
 *
 */
public class DataSourceFactory {

	private final static Log LOGGER = LogFactory.getLog(DataSourceFactory.class);

	private static final String SHARE_PREFIX = "_sharedConnection.";

	private DataSourceFactory() {
		// Utility class
	}

	/**
	 * Builds a pooled dataSource from a connection configuration pattern.
	 *
	 * A bit weird naming, because connections are usually obtained from dataSources, but to keep backwards compatibility we stick to this
	 * naming.
	 *
	 * @param connectionName
	 *            a connection name as found in the .properties file
	 * @param context
	 *            the context containing properties
	 * @return a dataSource as configured in the .properties file
	 */
	public static DataSource buildDataSource(final String connectionName, final IContext context) throws ETLException {
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

		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource();

		singleConnectionDataSource.setDriverClassName(driverClassName);
		singleConnectionDataSource.setUrl(url);
		singleConnectionDataSource.setUsername(username);
		singleConnectionDataSource.setPassword(password);
		singleConnectionDataSource.setAutoCommit(false);

		// TODO Bogdan - basicDataSource.setMaxActive(100);
		// TODO Bogdan - basicDataSource.setMaxIdle(30);
		// TODO Bogdan - basicDataSource.setMaxWait(10000);

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("Built a new dataSource: [url=" + singleConnectionDataSource.getUrl() + ", username="
					+ singleConnectionDataSource.getUsername() + "]");
		}

		return singleConnectionDataSource;
	}

	/**
	 * Sets the connection with specified shareName in context, overwriting the one that could possibly exist under the same name.
	 * 
	 * @param context
	 * @param shareName
	 * @param connection
	 */
	public static void setSharedConnection(final IContext context, final String shareName, final Connection connection) {
		context.setData(SHARE_PREFIX + shareName, connection);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Stored shared connection with name: " + SHARE_PREFIX + shareName + " into the context.");
		}
	}

}
