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
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;

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
		
		Connection con = (Connection) context.getData(SHARE_PREFIX + shareName);
		if (con == null || con.isClosed()) {
			con = openConnection(context, connectName);
			setSharedConnection(context, shareName, con);
		}
		
		return con;
		
	}

	/**
	 * Sets the connection with specified shareName in context without checking if it already exists.
	 * @param context
	 * @param shareName
	 * @param connection
	 */
	public static void setSharedConnection(IContext context, String shareName, Connection connection) {
		context.setData(SHARE_PREFIX + shareName, connection);
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
		
		String driver = context.getProperty(name + ".connection.driver", "net.sourceforge.jtds.jdbc.Driver");
		String url = context.getProperty(name + ".connection.url");
		String username = context.getProperty(name + ".connection.username");
		String password = context.getProperty(name + ".connection.password");
		int isolationLevel = context.getPropertyInt(name + ".connection.transactionIsolation", -1);
		
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
		
		log.info("Opening new JDBC connection to: " + url);
		
		Connection con;
		try {
			con = DriverManager.getConnection(url, username, password);
		} catch (UnsatisfiedLinkError ule) {
			if (ule.getMessage().contains("java.library.path")) {
				String libPath = System.getProperty("java.library.path");
				throw new ETLException("Problem loading linking library from lava.library.path: " + libPath, ule);
			} else {
				throw new ETLException(ule);
			}
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
		
		log.info("RDBMS: " + databaseName + ", version: " + meta.getDatabaseProductVersion().replace("\n", ", ") + 
				", JDBC driver: " + meta.getDriverName() + ", version: " + meta.getDriverVersion() + ", " + transIso);
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
