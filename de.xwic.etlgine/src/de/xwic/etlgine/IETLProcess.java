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
public interface IETLProcess extends IProcess {

	/**
	 * @return the extractor
	 */
	public IExtractor getExtractor();

	/**
	 * @param extractor the IExtractor to set
	 */
	public void setExtractor(IExtractor extractor);

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
	 * Add a transformer at specified index.
	 * @param transformer
	 * @param index
	 */
	public void addTransformer(ITransformer transformer, int index);

	/**
	 * Returns the list of transformers.
	 * @return
	 */
	public List<ITransformer> getTransformers();

	/**
	 * @return the stopAfterRecords
	 */
	public int getStopAfterRecords();

	/**
	 * @param stopAfterRecords the stopAfterRecords to set
	 */
	public void setStopAfterRecords(int stopAfterRecords);
	
}
