package de.xwic.etlgine.loader.database;

import java.sql.Connection;
import java.sql.SQLException;

import de.xwic.etlgine.ETLException;

/**
 *
 * @author mbogdan
 */
public class ConnectionUtils {

	/**
	 * Commits and closes a connection.
	 * 
	 * @param connection
	 * @throws ETLException
	 */
	public static void commitConnection(final Connection connection) throws ETLException {
		try {
			if (connection == null) {
				return;
			}
			connection.commit();
		} catch (SQLException e) {
			throw new ETLException("Cannot commit connection.", e);
		} finally {
			try {
				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (SQLException e) {
				throw new ETLException("Cannot close connection.", e);
			}
		}
	}

	/**
	 * Rolls back and closes a connection.
	 * 
	 * @param connection
	 * @throws ETLException
	 */
	public static void rollbackConnection(final Connection connection) throws ETLException {
		try {
			if (connection == null) {
				return;
			}
			connection.rollback();
		} catch (SQLException e) {
			throw new ETLException("Cannot rollback connection.", e);
		} finally {
			try {
				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (SQLException e) {
				throw new ETLException("Cannot close connection.", e);
			}
		}
	}

}
