/*
 * de.xwic.etlgine.impl.AbstractLoader 
 */
package de.xwic.etlgine;


/**
 * @author lippisch
 */
public abstract class AbstractExtractor implements IExtractor {

	protected IContext context = null;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IContext context) {
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	public void onProcessFinished(IContext context) throws ETLException {
		
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IContext context) throws ETLException {
		
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IContext context) throws ETLException {
		
	}
}
