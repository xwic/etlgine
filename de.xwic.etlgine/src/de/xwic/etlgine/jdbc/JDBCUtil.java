/**
 * 
 */
package de.xwic.etlgine.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;

/**
 * @author Developer
 */
public class JDBCUtil {

	private static final String SHARE_PREFIX = "_sharedConnection.";
	
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
			context.setData(SHARE_PREFIX + shareName, con);
		}
		
		return con;
		
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
		
		return DriverManager.getConnection(url, username, password);
		
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


}
