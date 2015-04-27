/*
 * de.xwic.etlgine.impl.EtlProcess 
 */
package de.xwic.etlgine.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.ILoader;
import de.xwic.etlgine.IMonitor.EventType;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.ITransformer;
import de.xwic.etlgine.Result;

/**
 * @author lippisch
 */
public class ETLProcess extends Process implements IETLProcess {

	protected List<ISource> sources = new ArrayList<ISource>();
	protected List<ITransformer> transformers = new ArrayList<ITransformer>();
	protected List<ILoader> loaders = new ArrayList<ILoader>();
	protected IExtractor extractor = null;
	protected int stopAfterRecords = 0;
	protected boolean skipInvalidRecords = true;
	protected String scriptFilename = null;

	/**
	 * @param context
	 * @param name
	 */
	public ETLProcess(IContext context, String name) {
		super(context, name);
	}

	/**
	 * @param name
	 */
	public ETLProcess(String name) {
		super(name);
	}

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
	 * Add a transformer.
	 * @param transformer
	 */
	public void addTransformer(ITransformer transformer) {
		transformers.add(transformer);
	}
	
	@Override
	public void addTransformer(ITransformer transformer, int index) {
		transformers.add(index, transformer);
	}
	
	/**
	 * Returns the list of transformers.
	 * @return
	 */
	public List<ITransformer> getTransformers() {
		return Collections.unmodifiableList(transformers);
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
	 * @return 
	 */
	@Override
	public Result start() throws ETLException {
		
		result = Result.FAILED;
		
		if (sources.size() == 0) {
			throw new ETLException("No sources defined in process '" + name + "'.");
		}
		if (extractor == null) {
			throw new ETLException("No extractor defined in process '" + name + "'.");
		}
		
		monitor.logInfo("Starting process '" + name + "'");
		monitor.onEvent(processContext, EventType.PROCESS_START, this);
		
		try {
			// initialize the extractor
			monitor.logInfo("Initializing extractor " + extractor);
			extractor.initialize(processContext);
			// initialize transformer
			for (ITransformer transformer : transformers) {
				monitor.logInfo("Initializing transformer " + transformer);
				transformer.initialize(processContext);
			}
			// initialize loaders 
			for (ILoader loader : loaders) {
				monitor.logInfo("Initializing loader " + loader);
				loader.initialize(processContext);
			}
			
			// iterate over sources, allow dynamic addition
			for (int sourceIdx = 0; sourceIdx < sources.size(); sourceIdx++) {
				ISource source = sources.get(sourceIdx);
				
				if (!source.isAvailable()) {
					if (source.isOptional()) {
						monitor.logWarn("The optional source " + source.getName() + " is not available.");
					} else {
						String msg = "The mandatory source " + source.getName() + " is not availble. Import aborted.";
						monitor.logError(msg);
						throw new ETLException(msg);
					}
				} else {
					processContext.setCurrentSource(source);
					
					// let the loader open the source
					IDataSet dataSet = new DataSet();
					processContext.setDataSet(dataSet);
					

					// invoke preSourceOpening event methods.
					extractor.preSourceOpening(processContext);
					for (ITransformer transformer : transformers) {
						transformer.preSourceOpening(processContext);
					}
					for (ILoader loader : loaders) {
						loader.preSourceOpening(processContext);
					}

					monitor.logInfo("Opening source " + source.getName());
					
					extractor.openSource(source, dataSet);
					
					monitor.onEvent(processContext, EventType.SOURCE_POST_OPEN, this);

					extractor.preSourceProcessing(processContext);
					
					// initialize loaders by source
					for (ITransformer transformer : transformers) {
						transformer.preSourceProcessing(processContext);
					}
					// initialize loaders by source
					for (ILoader loader : loaders) {
						loader.preSourceProcessing(processContext);
					}
					
					// iterate over records
					IRecord nextRecord = null;
					for (IRecord record = extractor.getNextRecord(); record != null; record = nextRecord) {
						nextRecord = extractor.getNextRecord();
						if (nextRecord == null) {
							record.setHasNext(false);
						}
						
						if ((!record.isInvalid() || !skipInvalidRecords) && (!record.isSkip())) {
							for (ITransformer transformer : transformers) {
								List<IRecord> duplicates = record.getDuplicates();
								int duplicatesSize = duplicates.size();
								transformer.processRecord(processContext, record);
								// process duplicates
								for (int i = 0; i < duplicatesSize; i++) {
									IRecord duplicate = duplicates.get(i);
									if (!duplicate.isInvalid() || !skipInvalidRecords) {
										transformer.processRecord(processContext, duplicate);
									}
								}
							}
						}
						if (record.isInvalid()) {
							monitor.logWarn("Invalid record : " + record.getInvalidReason());
						} else if (!record.isSkip()) {
							for (ILoader loader : loaders) {
								loader.processRecord(processContext, record);
								if (record.isInvalid()) {
									monitor.logWarn("Invalid record : " + record.getInvalidReason());
									break;
								}
								// process duplicates
								for (IRecord duplicate : record.getDuplicates()) {
									loader.processRecord(processContext, duplicate);
									if (duplicate.isInvalid()) {
										monitor.logWarn("Invalid record : " + duplicate.getInvalidReason());
										break;
									}
								}
							}
						}
						if (record.isInvalid() && skipInvalidRecords) {
							record.setSkip(true);
						}
						
						processContext.recordProcessed(record);
						// TODO track duplicates as well and inform
						monitor.onEvent(processContext, EventType.RECORD_PROCESSED, this);
						
						if (stopAfterRecords > 0 && processContext.getRecordsCount() >= stopAfterRecords) {
							monitor.logWarn("Stopped after " + stopAfterRecords + " records because of stop condition.");
							break;
						}
						
						if (processContext.isStopFlag()) {
							break; 
						}
						
					}
					
					// notify transformers that the source processing is done.
					for (ITransformer transformer : transformers) {
						transformer.postSourceProcessing(processContext);
					}
					// notify loaders the the source processing is done.
					for (ILoader loader : loaders) {
						loader.postSourceProcessing(processContext);
					}
					
					extractor.postSourceProcessing(processContext);
					monitor.onEvent(processContext, EventType.SOURCE_FINISHED, this);
				}
				
				if (processContext.isStopFlag()) {
					break;
				}
				
			}

			// notify transformers and loaders that we are done.
			for (ITransformer transformer : transformers) {
				transformer.onProcessFinished(processContext);
			}
			for (ILoader loader : loaders) {
				loader.onProcessFinished(processContext);
			}
			
			extractor.onProcessFinished(processContext);
			// moved to finally part
			//monitor.onEvent(processContext, EventType.PROCESS_FINISHED);
			result = processContext.isStopFlag() ? Result.FAILED : Result.SUCCESSFULL;
		} catch (ETLException e) {
			//monitor.logError("Error during ETL processing: " + e, e);
			result = Result.FAILED;
			processContext.setLastException(e);
			throw e;
		} catch (Throwable t) {
			result = Result.FAILED;
			processContext.setLastException(t);
			throw new ETLException("Error during ETL processing: " + t, t);
		} finally {
			processContext.setResult(result);
			// close everything
			if (extractor != null) {
				extractor.close();
			}
			// run finalizers, allow modification during the loop
			for (int i = 0; i < finalizers.size(); i++) {
				IProcessFinalizer finalizer = finalizers.get(i);
				try {
					finalizer.onFinish(processContext);
				} catch (Throwable t) {
					monitor.logError("Error executing finalizer!", t);
					if (null == processContext.getLastException()){
						processContext.setLastException(t);
					}
					throw new ETLException("Error during ETL process finalizer: " + t, t);
				}
			}
			
			// copy back the result, as it might have changed
			// by a finalizer that failed.
			result = processContext.getResult();
			
			monitor.onEvent(processContext, EventType.PROCESS_FINISHED, this);
		}
		
		return result;
	}

	/**
	 * @return the stopAfterRecords
	 */
	public int getStopAfterRecords() {
		return stopAfterRecords;
	}

	/**
	 * @param stopAfterRecords the stopAfterRecords to set
	 */
	public void setStopAfterRecords(int stopAfterRecords) {
		this.stopAfterRecords = stopAfterRecords;
	}

	/**
	 * @return the skipInvalidRecords
	 */
	public boolean isSkipInvalidRecords() {
		return skipInvalidRecords;
	}

	/**
	 * @param skipInvalidRecords the skipInvalidRecords to set
	 */
	public void setSkipInvalidRecords(boolean skipInvalidRecords) {
		this.skipInvalidRecords = skipInvalidRecords;
	}

	/**
	 * @return the scriptFilename
	 */
	public String getScriptFilename() {
		return scriptFilename;
	}

	/**
	 * @param scriptFilename the scriptFilename to set
	 */
	public void setScriptFilename(String scriptFilename) {
		this.scriptFilename = scriptFilename;
	}

	/**
	 * Like Class.getResourceAsStream(String) returns the resource if name is not found as a file
	 * in script directory or in file system (for absolute name).
	 * @param name
	 * @return
	 */
	public InputStream getResourceAsStream(String name) {
		File file = new File(name);
		if (!file.isAbsolute() && getScriptFilename() != null) {
			file = new File(new File(getScriptFilename()).getParentFile(), name);
		}
		if (file.exists()) {
			try {
				return new BufferedInputStream(new FileInputStream(file));
			} catch (FileNotFoundException e) {
				// return null if not found
				return null;
			}
		}
		return getClass().getResourceAsStream(name);
	}
	
	/**
	 * Provides a OutputStream for given name.
	 * @param name
	 * @return
	 */
	public OutputStream getResourceAsOutputStream(String name) {
		File file = new File(name);
		if (!file.isAbsolute() && getScriptFilename() != null) {
			file = new File(new File(getScriptFilename()).getParentFile(), name);
		}
		try {
			return new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// return null if not found
			return null;
		}
	}
}
