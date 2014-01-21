/**
 * 
 */
package de.xwic.etlgine.finalizer;

import de.xwic.cube.IDataPool;
import de.xwic.cube.StorageException;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJobFinalizer;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.loader.cube.IDataPoolProvider;

/**
 * @author JBORNEMA
 *
 */
public class SaveDataPoolFinalizer implements IJobFinalizer {

	private IDataPoolProvider dataPoolProvider;
	private boolean saveOnError = false;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJobFinalizer#onFinish(de.xwic.etlgine.IJob)
	 */
	public void onFinish(IJob job) throws ETLException {
		IMonitor monitor = job.getProcessChain().getMonitor();
		if (job.getState() == State.ERROR) {
			if (saveOnError) {
				monitor.logWarn("Storing DataPool with job error state");
			} else {
				monitor.logError("Storing DataPool aborted");
				return;
			}
		}
		monitor.logInfo("Storing DataPool...");
		IDataPool dataPool = dataPoolProvider.getDataPool(job.getProcessChain().getGlobalContext());
		try {
			dataPool.save();
		} catch (StorageException e) {
			throw new ETLException(e);
		}
		monitor.logInfo("Storing DataPool finished...");
	}

	/**
	 * @return the dataPoolProvider
	 */
	public IDataPoolProvider getDataPoolProvider() {
		return dataPoolProvider;
	}

	/**
	 * @param dataPoolProvider the dataPoolProvider to set
	 */
	public void setDataPoolProvider(IDataPoolProvider dataPoolProvider) {
		this.dataPoolProvider = dataPoolProvider;
	}

	/**
	 * @return the saveOnError
	 */
	public boolean isSaveOnError() {
		return saveOnError;
	}

	/**
	 * @param saveOnError the saveOnError to set
	 */
	public void setSaveOnError(boolean saveOnError) {
		this.saveOnError = saveOnError;
	}
}
