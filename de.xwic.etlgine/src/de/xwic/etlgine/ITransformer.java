/*
 * de.xwic.etlgine.ITransformer 
 */
package de.xwic.etlgine;

/**
 * Is able to transform the data.
 * @author lippisch
 */
public interface ITransformer extends IETLProcessParticipant {

	/**
	 * Apply transformations to a record.
	 * @param context
	 * @param record
	 */
	public void processRecord(IETLContext context, IRecord record);
	
}
