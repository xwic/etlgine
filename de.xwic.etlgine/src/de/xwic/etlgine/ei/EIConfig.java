/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import de.xwic.etlgine.util.XmlUtil;

/**
 * Stores the EI configuration.
 * @author Lippisch
 */
public class EIConfig {

	private Map<String, ConnectionConfig> connections = new HashMap<String, ConnectionConfig>();
	private Map<String, ConnectorConfig> connectors = new HashMap<String, ConnectorConfig>();
	
	/**
	 * @param file
	 * @throws DocumentException 
	 */
	public EIConfig(File file) throws IOException, DocumentException {

		if (!file.exists()) {
			throw new FileNotFoundException("The specified files does not exist. (" + file.getName() + ")");
		}
		
		Document doc = new SAXReader().read(file);
		parse(doc);

	}
	
	/**
	 * @param file
	 * @throws DocumentException 
	 */
	public EIConfig(InputStream in) throws IOException, DocumentException {
		if (in == null) {
			throw new NullPointerException("InputStream 'in' must not be mull.");
		}
		
		Document doc = new SAXReader().read(in);
		parse(doc);

	}
	
	/**
	 * Returns the connection details with the specified ID. 
	 * 
	 * @throws IllegalArgumentException if no connection is defined with the specified ID
	 * @param id
	 * @return
	 */
	public ConnectionConfig getConnectionDetail(String id) throws IllegalArgumentException {
		if (!connections.containsKey(id)) {
			throw new IllegalArgumentException("A connection with the id '" + id + "' is not defined.");
		}
		return connections.get(id);
	}
	
	/**
	 * Returns the specified connection details.
	 * @return
	 */
	public Collection<ConnectionConfig> getConnectionConfigs() {
		return connections.values();
	}
	
	/**
	 * Returns the connector with the specified id or throws an exception if that 
	 * connector is not registered.
	 * @param id
	 * @return
	 */
	public ConnectorConfig getConnectorConfig(String id) {
		if (!connectors.containsKey(id)) {
			throw new IllegalArgumentException("A connector with the id '" + id + "' is not defined.");
		}
		return connectors.get(id);
	}
	
	/**
	 * Returns the list of connector configurations.
	 * @return
	 */
	public Collection<ConnectorConfig> getConnectorConfigs() {
		return connectors.values();
	}
	
	/**
	 * Parse the document.
	 * @param doc
	 * @throws DocumentException 
	 */
	@SuppressWarnings("unchecked")
	private void parse(Document doc) throws DocumentException {
		
		Element root = doc.getRootElement();
		Element eConnections = root.element("connections");
		if (eConnections != null) {
			
			// load connections
			for (Iterator<Element> it = eConnections.elementIterator("connection"); it.hasNext(); ) {
				Element eCon = it.next();
				String id = eCon.attributeValue("id");
				if (id == null) {
					throw new DocumentException("must specify id for a connection");
				}
				if (connections.containsKey(id)) {
					throw new DocumentException("The id '" + id + "' is specified already.");
				}
				
				ConnectionConfig conConfig = new ConnectionConfig(id);
				conConfig.setTraced("true".equalsIgnoreCase(eCon.attributeValue("traced")));
				conConfig.setPooled("true".equalsIgnoreCase(eCon.attributeValue("pooled")));
				conConfig.setLazyInit(!"false".equalsIgnoreCase(eCon.attributeValue("lazyInit")));

				XmlUtil.elementToBean(eCon, conConfig, false);
				
				// validate
				if (conConfig.getDriver() == null) {
					throw new DocumentException("driver not specified for connection id " + id);
				}
				
				connections.put(id, conConfig);
				
			}
			
		}
		
		// load connectors
		Element eConnectors = root.element("connectors");
		if (eConnectors != null) {
			
			// load connections
			for (Iterator<Element> it = eConnectors.elementIterator("connector"); it.hasNext(); ) {
				Element eCon = it.next();
				String id = eCon.attributeValue("id");
				if (id == null) {
					throw new DocumentException("must specify id for a connector");
				}
				if (connectors.containsKey(id)) {
					throw new DocumentException("The id '" + id + "' is specified already.");
				}
				
				ConnectorConfig conConfig = new ConnectorConfig(id);
				XmlUtil.elementToBean(eCon, conConfig, false);
				
				connectors.put(id, conConfig);

				if (conConfig.getClassName() == null || conConfig.getClassName().length() == 0) {
					throw new IllegalArgumentException("A className must be specified for connector with the id " + id);
				}
			}
			
		}

		
	}


}
