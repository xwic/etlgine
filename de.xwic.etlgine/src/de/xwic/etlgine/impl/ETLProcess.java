/*
 * de.xwic.etlgine.impl.EtlProcess 
 */
package de.xwic.etlgine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.ILoader;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.IMonitor.EventType;

/**
 * @author lippisch
 */
public class ETLProcess implements IETLProcess {

	protected List<ISource> sources = new ArrayList<ISource>();
	protected List<ILoader> loaders = new ArrayList<ILoader>();
	protected IExtractor extractor = null;
	protected IMonitor monitor = new DefaultMonitor();

	/**
	 * Add a source.
	 * @param source
	 */
	public void addSource(ISource source) {
		sources.add(source);
	}
	
	/**
	 * Returns the list of specified sources.
	 * @return
	 */
	public List<ISource> getSources() {
		return Collections.unmodifiableList(sources);
	}
	
	/**
	 * Add a loader.
	 * @param loader
	 */
	public void addLoader(ILoader loader) {
		loaders.add(loader);
	}
	
	/**
	 * Returns the list of loaders.
	 * @return
	 */
	public List<ILoader> getLoaders() {
		return Collections.unmodifiableList(loaders);
	}
	
	/**
	 * @return the loader
	 */
	public IExtractor getExtractor() {
		return extractor;
	}

	/**
	 * @param loader the loader to set
	 */
	public void setExtractor(IExtractor extractor) {
		this.extractor = extractor;
	}
	
	/**
	 * Start the process.
	 */
	public void start() throws ETLException {
		
		if (sources.size() == 0) {
			throw new ETLException("No sources defined.");
		}
		if (extractor == null) {
			throw new ETLException("No loader defined.");
		}
		
		// create the context
		Context context = new Context();
		
		try {
			// initialize the extractor
			extractor.initialize(context);
			// initialize loaders 
			for (ILoader loader : loaders) {
				loader.initialize(context);
			}
			
			// iterate over sources
			for (ISource source : sources) {
				
				if (!source.isAvailable()) {
					if (source.isOptional()) {
						monitor.logWarn("The optional source " + source.getName() + " is not available.");
					} else {
						monitor.logError("The mandatory source " + source.getName() + " is not availble. Import aborted.");
					}
				} else {
					
					// let the loader open the source
					IDataSet dataSet = new DataSet();
					context.setDataSet(dataSet);
					extractor.openSource(source, dataSet);
					
					monitor.onEvent(context, EventType.SOURCE_POST_OPEN);

					// initialize loaders by source
					for (ILoader loader : loaders) {
						loader.preSourceProcessing(context);
					}
					
					// iterate over records
					IRecord record;
					while ((record = extractor.getNextRecord()) != null) {
						
						for (ILoader loader : loaders) {
							loader.processRecord(context, record);
						}
						
						context.recordProcessed();
					}
					
					// notify loaders the the source processing is done.
					for (ILoader loader : loaders) {
						loader.postSourceProcessing(context);
					}
					monitor.onEvent(context, EventType.SOURCE_FINISHED);
				}
				
			}

			for (ILoader loader : loaders) {
				loader.onProcessFinished(context);
			}
			monitor.onEvent(context, EventType.PROCESS_FINISHED);
			
		} finally {
			// close everything
			if (extractor != null) {
				extractor.close();
			}
		}
		
	}

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor() {
		return monitor;
	}

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor) {
		this.monitor = monitor;
	}
	
}