/**
 * 
 */
package de.xwic.etlgine.extractor;

/**
 * @author lippisch
 *
 */
public interface ICSVExtractorListener {

	/**
	 * A line has been skipped. 
	 * @param line
	 * @param idx
	 */
	public void onLineSkipped(String line, int idx);
	
}
