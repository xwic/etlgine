/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A connector provides a logical abstracted access to foreign systems.
 * @author lippisch
 */
public interface IConnector {

	/**
	 * Initialize the connector. The connection object is null if
	 * no connection id was specified in the connector configuration.
	 * @param config
	 * @throws EIException 
	 */
	public void initialize(ConnectorConfig config, Connection connection, Credentials credentials) throws EIException;
	
	/**
	 * Close the connector.
	 * @throws SQLException 
	 * @throws EIException 
	 */
	public void close() throws EIException;
	
}
