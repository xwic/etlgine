/*
 * de.xwic.etlgine.impl.AbstractLoader 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLContext;
import de.xwic.etlgine.IExtractor;

/**
 * @author lippisch
 */
public abstract class AbstractExtractor implements IExtractor {

	protected IETLContext context = null;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IETLContext context) {
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	public void onProcessFinished(IETLContext context) throws ETLException {
		
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IETLContext context) throws ETLException {
		
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IETLContext context) throws ETLException {
		
	}
}
