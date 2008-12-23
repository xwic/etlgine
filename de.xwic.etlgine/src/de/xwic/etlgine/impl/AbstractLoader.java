/**
 * 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.ILoader;

/**
 * @author Lippisch
 */
public abstract class AbstractLoader implements ILoader {

	protected IContext context;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IContext context) throws ETLException {
		this.context = context;

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
