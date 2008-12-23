package de.xwic.etlgine;



/**
 * Loads the data into its target destination. This might be something like a file, database
 * or cube. 
 * @author lippisch
 *
 */
public interface ILoader extends IProcessParticipant {

	/**
	 * Load the record.
	 * @param context
	 * @param record
	 * @throws ETLException 
	 */
	public void processRecord(IContext context, IRecord record) throws ETLException;

	
}
