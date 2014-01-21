/*
 * de.xwic.etlgine.impl.AbstractTransformer 
 */
package de.xwic.etlgine;


/**
 * Abstract class for ITransformer.
 * @author lippisch
 */
public abstract class AbstractTransformer implements ITransformer {

	protected transient IProcessContext processContext = null;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITransformer#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#initialize(de.xwic.etlgine.IETLContext)
	 */
	public void initialize(IProcessContext processContext) throws ETLException {
		this.processContext = processContext;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessParticipant#preSourceOpening(de.xwic.etlgine.IProcessContext)
	 */
	public void preSourceOpening(IProcessContext processContext) throws ETLException {
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#onProcessFinished(de.xwic.etlgine.impl.Context)
	 */
	public void onProcessFinished(IProcessContext processContext) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IETLProcessParticipant#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	public void preSourceProcessing(IProcessContext processContext) throws ETLException {
		// TODO Auto-generated method stub

	}

}
