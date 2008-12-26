/**
 * 
 */
package de.xwic.etlgine;

import junit.framework.TestCase;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;

/**
 * @author Developer
 *
 */
public class TestRndExtractor2JDBCTest extends TestCase {

	/**
	 * Test the loading of source data into a JDBC table.
	 * @throws ETLException 
	 */
	public void testJDBCLoader() throws ETLException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");
		IProcess process = pc.createProcess("jdbcLoad");

		process.addSource(new TestRndSource(10000));
		
		process.setExtractor(new TestRndExtractor());
		
		
		JDBCLoader jdbcLoader = new JDBCLoader();
		jdbcLoader.setCatalogName("etlgine_test");
		jdbcLoader.setConnectionUrl("jdbc:jtds:sqlserver://localhost/etlgine_test");
		jdbcLoader.setUsername("etlgine");
		jdbcLoader.setPassword("etl");
		jdbcLoader.setTablename("LOAD_TEST_RND");
		jdbcLoader.setAutoCreateColumns(true);
		
		jdbcLoader.addIgnoreableColumns("ID");
		
		process.addLoader(jdbcLoader);
		
		pc.start();

		//DataDump.printStructure(System.out, pool.getDimension("Area"));

		
	}
	
}
