/**
 * 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLContext;
import de.xwic.etlgine.ILoader;

/**
 * @author Lippisch
 */
public abstract class AbstractLoader implements ILoader {

	protected IETLContext context;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IETLContext context) throws ETLException {
		this.context = context;

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#onProcessFinished(de.xwic.etlgine.impl.Context)
	 */
	public void onProcessFinished(Context context) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IETLContext context) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IETLContext context) throws ETLException {

	}


}
