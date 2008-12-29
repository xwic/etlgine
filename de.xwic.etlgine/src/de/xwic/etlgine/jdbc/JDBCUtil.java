/**
 * 
 */
package de.xwic.etlgine.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;

/**
 * @author Developer
 */
public class JDBCUtil {

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
			throw new ETLException("The URL is not specified for this connection name.");
		}
		if (username == null) {
			throw new ETLException("The username is not specified for this connection name.");
		}
		if (password == null) {
			throw new ETLException("The password is not specified for this connection name.");
		}
		
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException cnfe) {
			throw new ETLException("Driver " + driver + " can not be found.");
		}
		
		return DriverManager.getConnection(url, username, password);
		
	}
	
}
