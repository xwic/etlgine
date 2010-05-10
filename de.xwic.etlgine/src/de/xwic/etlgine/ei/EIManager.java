/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The EI Manager initializes and controls all configurations and available connectors.
 * @author Lippisch
 */
public class EIManager {
	
	private final Log log = LogFactory.getLog(EIManager.class);

	private static EIManager instance = null;
	private final EIConfig config;
	
	private ICredentialStore credentialStore = null;
	private Map<String, ConnectionHandler> connectionHandlers = new HashMap<String, ConnectionHandler>();
	
	/**
	 * Private constructor forces single-instance policy.
	 */
	private EIManager(EIConfig config) {
		this.config = config;
		log.info("Initializing EIManager...");
		
	}

	/**
	 * Initialize the manager.
	 * @param config
	 */
	public static void initialize(EIConfig config) {
		if (instance != null) {
			throw new IllegalStateException("EIManager already initialized.");
		}
		if (config == null) {
			throw new NullPointerException("config must be not null.");
		}
		instance = new EIManager(config);
		
		// pre-init non lazy connections
		for (ConnectionConfig cd : config.getConnectionConfigs()) {
			instance.connectionHandlers.put(cd.getId(), new ConnectionHandler(cd));
		}
	}

	/**
	 * Returns the EIManager instance.
	 * @return
	 */
	public static EIManager getInstance() {
		if (instance == null) {
			throw new IllegalStateException("The EIManager is not yet initialized.");
		}
		return instance;
	}
	
	/**
	 * Returns the configuration.
	 * @return
	 */
	public EIConfig getConfig() {
		return config;
	}
	
	/**
	 * Returns a list of open/active connection handlers.
	 * @return
	 */
	public Collection<ConnectionHandler> getConnectionHandlers() {
		return connectionHandlers.values();
	}
	
	/**
	 * Returns the ConnectionHandler with the specified id.
	 * @param id
	 * @return
	 */
	public ConnectionHandler getConnectionHandler(String id) {
		
		ConnectionHandler cn = connectionHandlers.get(id);
		if (cn == null) {
			throw new IllegalArgumentException("Connection ID is unknown.");
		}
		return cn;
		
	}
	
	/**
	 * Returns a connection of the specified id. 
	 * @param connectionId
	 * @return
	 * @throws SQLException 
	 */
	public Connection getConnection(String id) throws SQLException {
		return getConnectionHandler(id).getConnection();
	}

	/**
	 * Returns an initialized connector with the specified id. Make sure to close it after use.
	 * @param id
	 * @return
	 * @throws EIException
	 */
	public IConnector getConnector(String id) throws EIException {
		
		ConnectorConfig cfg = config.getConnectorConfig(id);
		IConnector connector = null;
		try {
			Class<?> clazz = Class.forName(cfg.getClassName());
			connector = (IConnector) clazz.newInstance();
		} catch (Exception e) {
			throw new EIException("Error instantiating the connector class " + cfg.getClassName() + ": " + e, e);
		}
		
		
		Connection con = null;
		if (cfg.getConnectionId() != null) {
			try {
				con = getConnection(cfg.getConnectionId());
			} catch (SQLException e) {
				throw new EIException("Error in getConnection for connector id " + cfg.getId());
			}
		}
		
		Credentials credentials = null;
		if (cfg.getCredentials() != null) {
			if (credentialStore == null) {
				throw new EIException("Credential ids are specified but no credential store is set.");
			}
			credentials = credentialStore.getCredentials(cfg.getCredentials());
		}
		
		connector.initialize(cfg, con, credentials);
		return connector;
		
	}
	
	/**
	 * 
	 */
	public static void destroy() {
		if (instance != null) {
			instance._destroy();
			instance = null; 
		}
	}

	/**
	 * Destroy open connection handlers. 
	 */
	private void _destroy() {
		for (ConnectionHandler ch : connectionHandlers.values()) {
			try {
				ch.destroy();
			} catch (Exception e) {
				log.error("Error closing connection handler: " + ch.getConnectionId());
			}
		}
	}

	/**
	 * @return the credentialStore
	 */
	public ICredentialStore getCredentialStore() {
		return credentialStore;
	}

	/**
	 * @param credentialStore the credentialStore to set
	 */
	public void setCredentialStore(ICredentialStore credentialStore) {
		this.credentialStore = credentialStore;
	}
	
}
