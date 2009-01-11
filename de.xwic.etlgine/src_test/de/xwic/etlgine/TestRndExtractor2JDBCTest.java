/**
 * 
 */
package de.xwic.etlgine;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;

import de.xwic.etlgine.extractor.jdbc.JDBCExtractor;
import de.xwic.etlgine.extractor.jdbc.JDBCSource;
import de.xwic.etlgine.loader.csv.CSVLoader;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;

/**
 * @author Developer
 *
 */
public class TestRndExtractor2JDBCTest extends TestCase {

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		BasicConfigurator.configure();
	}
	
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
	
	/**
	 * Read from a table.
	 */
	public void testJDBCExtractor() throws Exception {
		IProcessChain pc = ETLgine.createProcessChain("testChain");
		IProcess process = pc.createProcess("jdbcExtract");
		
		JDBCSource source = new JDBCSource();
		source.setConnectionUrl("jdbc:jtds:sqlserver://localhost/etlgine_test");
		source.setUsername("etlgine");
		source.setPassword("etl");
		source.setSqlSelectString("SELECT * FROM LOAD_TEST_RND");
		process.addSource(source);
		
		process.setExtractor(new JDBCExtractor());
		
		CSVLoader csvLoader = new CSVLoader();
		csvLoader.setFilename("test/jdbc_extract.csv");
		
		process.addLoader(csvLoader);
		
		pc.start();

	}
	
	
}
