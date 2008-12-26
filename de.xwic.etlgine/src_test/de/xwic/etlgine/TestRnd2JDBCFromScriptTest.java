/**
 * 
 */
package de.xwic.etlgine;

import java.io.FileNotFoundException;

import junit.framework.TestCase;

/**
 * @author Developer
 *
 */
public class TestRnd2JDBCFromScriptTest extends TestCase {
	
	/**
	 * Test the loading of source data into a JDBC table.
	 * @throws ETLException 
	 * @throws FileNotFoundException 
	 */
	public void testScript() throws ETLException, FileNotFoundException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");

		pc.createProcessFromScript("scripts/TestRnd2JDBC.groovy");
		
		pc.start();

		
	}

}
