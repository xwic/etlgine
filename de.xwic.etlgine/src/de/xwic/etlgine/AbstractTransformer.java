/*
 * de.xwic.etlgine.impl.AbstractTransformer 
 */
package de.xwic.etlgine;


/**
 * Abstract class for ITransformer.
 * @author lippisch
 */
public abstract class AbstractTransformer implements ITransformer {

	protected IContext context = null;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITransformer#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IContext context, IRecord record) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IContext context) throws ETLException {
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#onProcessFinished(de.xwic.etlgine.impl.Context)
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
		// TODO Auto-generated method stub

	}

}
