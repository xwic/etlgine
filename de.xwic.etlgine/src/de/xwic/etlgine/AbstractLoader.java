/**
 * 
 */
package de.xwic.etlgine;


/**
 * @author Lippisch
 */
public abstract class AbstractLoader implements ILoader {

	protected transient IProcessContext processContext;
	protected IMonitor monitor;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IProcessContext processContext) throws ETLException {
		this.processContext = processContext;
		monitor = processContext.getMonitor();

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessParticipant#preSourceOpening(de.xwic.etlgine.IProcessContext)
	 */
	public void preSourceOpening(IProcessContext processContext)
			throws ETLException {
		
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	public void onProcessFinished(IProcessContext processContext) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {

	}


}
