/*
 * de.xwic.etlgine.ISource 
 */
package de.xwic.etlgine;

/**
 * The source for a loader. A source may be a file, database or other sources.
 * @author lippisch
 */
public interface ISource {

	/**
	 * Returns the name of the source.
	 * @return
	 */
	public String getName();
	
	/**
	 * Tests if the source is available.
	 * @return
	 */
	public boolean isAvailable();
	
	/**
	 * Returns true if this source is optional. If not, the ETL process
	 * will abort if the source is not available.
	 * @return
	 */
	public boolean isOptional();
	
}
