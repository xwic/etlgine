/**
 * 
 */
package de.xwic.etlgine;


/**
 * @author Lippisch
 */
public abstract class AbstractLoader implements ILoader {

	protected IContext context;
	protected IMonitor monitor;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IContext context) throws ETLException {
		this.context = context;
		monitor = context.getMonitor();

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	public void onProcessFinished(IContext context) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IContext context) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IContext context) throws ETLException {

	}


}
