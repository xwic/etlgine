/**
 * 
 */
package de.xwic.etlgine;

import de.xwic.etlgine.demo.DemoDatabaseUtil;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;

import java.io.FileNotFoundException;

/**
 * @author Developer
 *
 */
public class TestRnd2JDBCFromScriptTest extends TestCase {

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        BasicConfigurator.configure();
        DemoDatabaseUtil.prepareDB("org.sqlite.JDBC", "jdbc:sqlite:test/etlgine_test.db3");
    }

	/**
	 * Test the loading of source data into a JDBC table.
	 * @throws ETLException 
	 * @throws FileNotFoundException 
	 */
	public void testScript() throws ETLException, FileNotFoundException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");

		pc.createProcessFromScript("TestRnd2JDBC", "scripts/TestRnd2JDBC.groovy");
		
		pc.start();

		
	}

}
