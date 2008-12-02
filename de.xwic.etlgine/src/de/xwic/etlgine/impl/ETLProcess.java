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
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.IMonitor.EventType;

/**
 * @author lippisch
 */
public class ETLProcess implements IETLProcess {

	protected List<ISource> sources = new ArrayList<ISource>();
	protected IExtractor loader = null;
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
	 * @return the loader
	 */
	public IExtractor getLoader() {
		return loader;
	}

	/**
	 * @param loader the loader to set
	 */
	public void setLoader(IExtractor loader) {
		this.loader = loader;
	}
	
	/**
	 * Start the process.
	 */
	public void start() throws ETLException {
		
		if (sources.size() == 0) {
			throw new ETLException("No sources defined.");
		}
		if (loader == null) {
			throw new ETLException("No loader defined.");
		}
		
		// create the context
		Context context = new Context();
		
		// initialize the loader
		try {
			loader.initialize(context);
			
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
					loader.openSource(source, dataSet);
					
					monitor.onEvent(context, EventType.SOURCE_POST_OPEN);
					
					// iterate over records
					IRecord record;
					while ((record = loader.getNextRecord()) != null) {
						
						
						context.recordProcessed();
					}
					
					monitor.onEvent(context, EventType.SOURCE_FINISHED);
				}
				
			}
			monitor.onEvent(context, EventType.PROCESS_FINISHED);
			
		} finally {
			// close everything
			if (loader != null) {
				loader.close();
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
