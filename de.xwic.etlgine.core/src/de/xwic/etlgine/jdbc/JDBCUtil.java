/**
 * 
 */
package de.xwic.etlgine.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.server.JobQueue;
import oracle.jdbc.driver.OracleConnection;

/**
 * @author Developer
 */
public class JDBCUtil {

	private static final String SHARE_PREFIX = "_sharedConnection.";

	protected final static Log log = LogFactory.getLog(JDBCUtil.class);
	
	/**
	 * Returns the connection with the specified shareName. If the connection does not exist, one is
	 * created. The connection is stored in the context under the key _sharedConnection.[shareName].
	 * 
	 * A 
	 * 
	 * @param context
	 * @param shareName
	 * @param connectName
	 * @return
	 * @throws ETLException
	 * @throws SQLException
	 */
	public static Connection getSharedConnection(IContext context, String shareName, String connectName) throws ETLException, SQLException {
		
		String queueName = getThreadQueueName();
		String sharedKey = SHARE_PREFIX + shareName;
		if (!sharedKey.contains(queueName)){
			sharedKey += queueName;
		}
		Connection con = (Connection) context.getData(sharedKey);
		if (con == null || con.isClosed()) {
			if(log.isDebugEnabled()) {
				log.debug("Shared connection with name: " + sharedKey +
						" was not found, or already closed. Opening a new connection...");
			}
			con = openConnection(context, connectName);
			setSharedConnection(context, shareName, con);
		}
		
		return con;
		
	}
	
	/**
	 * Returns the queue name for this running thread extracted from the thread name
	 * 
	 * @return empty string if no queue name can be extracted from the thread name
	 */
	private static String getThreadQueueName() {
		String queueName = Thread.currentThread().getName();
		if (null != queueName && -1 != queueName.indexOf(JobQueue.QUEUE_THREAD_PREFIX)){
			queueName = queueName.substring(queueName.indexOf(JobQueue.QUEUE_THREAD_PREFIX)+JobQueue.QUEUE_THREAD_PREFIX.length());
		}else{
			queueName="";
		}
		return queueName;
	}

	/**
	 * Sets the connection with specified shareName in context without checking if it already exists.
	 * @param context
	 * @param shareName
	 * @param connection
	 */
	public static synchronized void setSharedConnection(IContext context, String shareName, Connection connection) {
		String queueName = getThreadQueueName();
		String sharedKey = SHARE_PREFIX + shareName;
		if (!sharedKey.contains(queueName)){
			sharedKey += queueName;
		}
		context.setData(sharedKey, connection);
		if(log.isDebugEnabled()) {
			log.debug("Stored shared connection with name: " + SHARE_PREFIX + shareName + " into the context.");
		}
	}
	
	/**
	 * Executes the given SQL statement and returns the value from the first column in the first row
	 * of the ResultSet or NULL if no result is given.
	 * A new connection is opened for the query and closed after processing.
	 * @param con
	 * @param select
	 * @return
	 * @throws SQLException
	 * @throws ETLException 
	 */
	public static Object executeSingleValueQuery(IContext context, String connectionName, String select) throws SQLException, ETLException {
		Connection con = openConnection(context, connectionName);
		try {
			return executeSingleValueQuery(con, select);
		} finally {
			con.close();
		}
	}
	
	/**
	 * Executes the given SQL statement and returns the value from the first column in the first row
	 * of the ResultSet or NULL if no result is given.
	 * @param con
	 * @param select
	 * @return
	 * @throws SQLException
	 */
	public static Object executeSingleValueQuery(Connection con, String select) throws SQLException {
		
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(select);
			if (rs.next()) {
				return rs.getObject(1);
			}
			return null;
		} finally {
			if (rs != null) {
				rs.close();
			}
			stmt.close();
		}
	}
	
	/**
	 * Open a pre-configured connection. The connection details are obtained from
	 * the context properties in the format: [name].connection.XXX
	 * @param context
	 * @param name
	 * @return
	 */
	public static Connection openConnection(IContext context, String name) throws ETLException, SQLException {
		return openConnection(context, name, true);
	}

	/**
	 * Open a pre-configured connection. The connection details are obtained from
	 * the context properties in the format: [name].connection.XXX
	 * @param context
	 * @param name
	 * @param logging
	 * @return
	 */
	public static Connection openConnection(IContext context, String name, boolean logging) throws ETLException, SQLException {
		
		String driver = context.getProperty(name + ".connection.driver", "net.sourceforge.jtds.jdbc.Driver");
		String url = context.getProperty(name + ".connection.url");
		String username = context.getProperty(name + ".connection.username");
		String password = context.getProperty(name + ".connection.password");
		int isolationLevel = context.getPropertyInt(name + ".connection.transactionIsolation", -1);
		
		Properties props = new Properties();
		props.setProperty("user", username);
		props.setProperty("password", password);
		if (driver.equals("oracle.jdbc.driver.OracleDriver")) {
			props = getConPropValue(context, name, props, logging);
		}
		if (url == null) {
			throw new ETLException("The URL is not specified for this connection name. ('" + name + "')");
		}
		if (username == null) {
			throw new ETLException("The username is not specified for this connection name. ('" + name + "')");
		}
		if (password == null) {
			throw new ETLException("The password is not specified for this connection name. ('" + name + "')");
		}
		
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException cnfe) {
			throw new ETLException("Driver " + driver + " can not be found.");
		}
		if (logging){
			log.info("Opening new JDBC connection to: " + url);
		}
		Connection con;
		try {
			con = DriverManager.getConnection(url, props);
		} catch (UnsatisfiedLinkError ule) {
			if (ule.getMessage().contains("java.library.path")) {
				String libPath = System.getProperty("java.library.path");
				throw new ETLException("Problem loading linking library from lava.library.path: " + libPath, ule);
			} else {
				throw new ETLException(ule);
			}
		} catch (SQLTimeoutException e) {
			log.error("Connection timeout occurred while trying to get connection!" + e);
			throw new ETLException("Connection timeout occurred while trying to get connection! " + e, e);
		}

		// set optional transaction isolation level
		if (isolationLevel != -1) {
			con.setTransactionIsolation(isolationLevel);
		}
		
		DatabaseMetaData meta = con.getMetaData();
		String databaseName = meta.getDatabaseProductName();

		String transIso = null;
		switch (con.getTransactionIsolation()) {
		case Connection.TRANSACTION_READ_UNCOMMITTED:
			transIso = "TRANSACTION_READ_UNCOMMITTED";
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			transIso = "TRANSACTION_READ_COMMITTED";
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			transIso = "TRANSACTION_REPEATABLE_READ";
			break;
		case Connection.TRANSACTION_SERIALIZABLE:
			transIso = "TRANSACTION_SERIALIZABLE";
			break;
		case Connection.TRANSACTION_NONE:
			transIso = "TRANSACTION_NONE";
			break;
		case 4096: //4096 corresponds to SQLServerConnection.TRANSACTION_SNAPSHOT
			transIso = "TRANSACTION_SNAPSHOT";
			break;
		default:
			transIso = "Unknown Transaction Isolation!";
			break;
		}
		if (logging){
		log.info("RDBMS: " + databaseName + ", version: " + meta.getDatabaseProductVersion().replace("\n", ", ") + 
				", JDBC driver: " + meta.getDriverName() + ", version: " + meta.getDriverVersion() + ", " + transIso);
		}
		return con;
		
		/*
		return new DelegatingConnection(con) {
			@Override
			public void commit() throws SQLException {
				super.commit();
			}
			@Override
			public void rollback() throws SQLException {
				super.rollback();
			}
			@Override
			public void close() throws SQLException {
				super.close();
			}
		};
		*/
	}

	/**
	 * Returns the batch size used for this jdbc statements.
	 * @param context
	 * @param name
	 * @return
	 */
	public static int getBatchSize(IContext context, String name) {
		int batch_size = context.getPropertyInt(name + ".connection.batch_size", 0);
		return batch_size;
	}
	
	/**
	 * Returns the fetch size used for this jdbc statements.
	 * @param context
	 * @param name
	 * @return
	 */
	public static int getFetchSize(IContext context, String name) {
		int fetch_size = context.getPropertyInt(name + ".connection.fetch_size", 0);
		return fetch_size;
	}

	/**
	 * Returns the Connection Timeout used for this jdbc statements.
	 * @param context
	 * @param name
	 * @return
	 */
	public static int getConStatementTimeout(IContext context, String name) {
		int constatementTimeout = context.getPropertyInt(name + ".connection.con_statement_timeout", 0);
		log.info("global con_statement_timeout value: " + context.getPropertyInt("global.connection.con_statement_timeout", 0)); 
		if(constatementTimeout==0)
			constatementTimeout = context.getPropertyInt("global.connection.con_statement_timeout", 0);
		
		return constatementTimeout;
	}
	
	/**
	 * If defining new property at connection level need to override the global connection property as well
	 * numOfProperty will give the number's of properties you want to add by default it is 1
	 * Returns Property
	 * @param context
	 * @param name
	 * @return
	 */
	public static Properties getConPropValue(IContext context, String name, Properties props, boolean logging) {

		int numOfProperty = 1;
		String propertyName = null, propertyValue = null;

		log.info("connection property name::" + name + ".connection.numOfProperty");
		if (context.getProperty(name + ".connection.numOfProperty") != null)
			numOfProperty = context.getPropertyInt(name + ".connection.numOfProperty", 1);
		else
			numOfProperty = context.getPropertyInt("global.connection.numOfProperty", 1);

		for (int i = 1; i <= numOfProperty; i++) {
			if (context.getProperty(name + ".connection.property" + i) != null)
				propertyName = context.getProperty(name + ".connection.property" + i);
			else if (context.getProperty("global.connection.property" + i) != null)
				propertyName = context.getProperty("global.connection.property" + i);
			if (propertyName != null) {
				if (context.getProperty(name + "." + propertyName) != null)
					propertyValue = context.getProperty(name + "." + propertyName);
				else if (context.getProperty("global." + propertyName) != null)
					propertyValue = context.getProperty("global." + propertyName);
				if (logging) {
					log.info("PropertyName:" + propertyName + "PropertyValue:" + propertyValue);
				}
				if (propertyValue != null)
					props.setProperty(propertyName, propertyValue);
			}
		}
		return props;
	}
	/**
	 * @param rs
	 * @throws SQLException 
	 */
	public static void dumpResultSet(ResultSet rs) throws SQLException {
		
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

	/**
	 * Checks if the specified column exists.
	 * @param con
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws SQLException
	 */
	public static boolean columnExists(Connection con, String tableName, String columnName) throws SQLException {
		
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet columns = metaData.getColumns(con.getCatalog(), null, tableName, columnName);
		try {
			if (columns.next()) {
				return true;
			}
			return false;
		} finally {
			columns.close();
		}
		
	}
	
	/**
	 * Checks if the specified column exists and return false if it does.
	 * If not the column is created and true returned.
	 * @param con
	 * @param tableName
	 * @param columnName
	 * @param sqlTypeDef
	 * @return
	 * @throws SQLException
	 */
	public static boolean ensureColumn(Connection con, String tableName, String columnName, String sqlTypeDef) throws SQLException {
		
		boolean exists = columnExists(con, tableName, columnName);
		if (exists) {
			// nothing to do
			return false;
		}
		
		// create column
		Statement stmt = con.createStatement();
		try {
			String sql = "alter table [" + tableName + "] add [" + columnName + "] " + sqlTypeDef;
			stmt.executeUpdate(sql);
			log.debug("Table column added: " + sql);
		} finally {
			stmt.close();
		}
		
		return true;
	}

	/**
	 * @param con
	 * @param string
	 * @throws SQLException 
	 */
	public static int executeUpdate(Connection con, String sql) throws SQLException {
		
		Statement stmt = con.createStatement();
		try {
			int result = stmt.executeUpdate(sql);
			return result;
		} finally {
			stmt.close();
		}
		
		
	}

	/**
	 * Returns the JDBC identifier separator.
	 * On SQLException " is used as fallback.
	 * @param connection
	 * @return
	 */
	public static String getIdentifierSeparator(Connection connection) {
		String identifierSeparator = null;
		try {
			identifierSeparator = connection.getMetaData().getIdentifierQuoteString();
		} catch (SQLException e) {
			identifierSeparator = "\""; // use this
			log.warn("Error reading identifierQuoteString", e);
		}
		return identifierSeparator;
	}

}
