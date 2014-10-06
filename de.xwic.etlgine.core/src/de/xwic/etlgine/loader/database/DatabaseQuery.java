package de.xwic.etlgine.loader.database;

import java.util.Map;

/**
 * Class holding an SQL query string and all the named parameters that need to be applied when executing.
 * 
 * @author mbogdan
 *
 */
public class DatabaseQuery {

	/** The SQL query string, passed as StringBuilder because some other operations could be done before executing */
	private StringBuilder queryString;

	/** The parameters used in the queryString, key is parameter name as in ":TRANSACTION_param" and value is the value to set */
	private Map<String, Object> parameters;

	public StringBuilder getQueryString() {
		return queryString;
	}

	public void setQueryString(StringBuilder queryString) {
		this.queryString = queryString;
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}
}
