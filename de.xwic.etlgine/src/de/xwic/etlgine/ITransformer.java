/*
 * de.xwic.etlgine.ITransformer 
 */
package de.xwic.etlgine;

/**
 * Is able to transform the data.
 * @author lippisch
 */
public interface ITransformer extends IProcessParticipant {

	/**
	 * Apply transformations to a record.
	 * @param processContext
	 * @param record
	 * @throws ETLException 
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException;
	
}
