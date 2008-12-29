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
public interface IProcess {

	/**
	 * @return the extractor
	 */
	public IExtractor getExtractor();

	/**
	 * @param extractor the IExtractor to set
	 */
	public void setExtractor(IExtractor extractor);

	/**
	 * Start the process. Usualy this method is invoked from the IProcessChain.
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

	/**
	 * Add a loader.
	 * @param loader
	 */
	public void addLoader(ILoader loader);

	/**
	 * Returns the list of loaders.
	 * @return
	 */
	public List<ILoader> getLoaders();

	/**
	 * Add a transformer.
	 * @param transformer
	 */
	public void addTransformer(ITransformer transformer);

	/**
	 * Returns the list of transformers.
	 * @return
	 */
	public List<ITransformer> getTransformers();

	/**
	 * @return the name
	 */
	public String getName();

	/**
	 * @return the context
	 */
	public IProcessContext getContext();

	/**
	 * Add a process finalizer.
	 */
	public void addProcessFinalizer(IProcessFinalizer finalizer);
	
	/**
	 * Returns the process finalizers.
	 * @return
	 */
	public List<IProcessFinalizer> getProcessFinalizers();
	
}
