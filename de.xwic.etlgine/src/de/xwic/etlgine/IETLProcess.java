/*
 * de.xwic.etlgine.IETLProcess 
 */
package de.xwic.etlgine;

import java.util.List;

/**
 * Defines the whole process of extracting, transformation and loading.
 * An ETL process has one IExtractor, none to many ITransformer and one
 * to many ILoader.
 * 
 * 
 * @author lippisch
 */
public interface IETLProcess {

	/**
	 * @return the loader
	 */
	public IExtractor getLoader();

	/**
	 * @param loader the loader to set
	 */
	public void setLoader(IExtractor loader);

	/**
	 * Start the process.
	 */
	public void start() throws ETLException;

	/**
	 * Add a source.
	 * @param source
	 */
	public void addSource(ISource source);

	/**
	 * Returns the list of specified sources.
	 * @return
	 */
	public List<ISource> getSources();

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

	
	
}
