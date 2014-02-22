/**
 * 
 */
package de.xwic.etlgine;

import de.xwic.etlgine.demo.DemoDatabaseUtil;
import de.xwic.etlgine.loader.jdbc.SqlDialect;
import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;

import de.xwic.etlgine.extractor.jdbc.JDBCExtractor;
import de.xwic.etlgine.extractor.jdbc.JDBCSource;
import de.xwic.etlgine.loader.csv.CSVLoader;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;

import java.sql.ResultSet;

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
        DemoDatabaseUtil.prepareDB("org.sqlite.JDBC","jdbc:sqlite:test/etlgine_test.db3");
	}
	
	/**
	 * Test the loading of source data into a JDBC table.
	 * @throws ETLException 
	 */
	public void testJDBCLoader() throws ETLException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");
		IETLProcess process = pc.createProcess("jdbcLoad");

		process.addSource(new TestRndSource(10000));
		
		process.setExtractor(new TestRndExtractor());
		
		
		JDBCLoader jdbcLoader = new JDBCLoader();
        jdbcLoader.setDriverName("org.sqlite.JDBC");
		jdbcLoader.setConnectionUrl("jdbc:sqlite:test/etlgine_test.db3");
        jdbcLoader.setUsername("");
        jdbcLoader.setPassword("");
		jdbcLoader.setTablename("LOAD_TEST_RND");
		jdbcLoader.setAutoCreateColumns(true);
		jdbcLoader.setSqlDialect(SqlDialect.SQLITE);
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
		IETLProcess process = pc.createProcess("jdbcExtract");
		
		JDBCSource source = new JDBCSource();
        source.setDriverName("org.sqlite.JDBC");
        source.setConnectionUrl("jdbc:sqlite:test/etlgine_test.db3");
        source.setUsername("");
        source.setPassword("");
		source.setSqlSelectString("SELECT * FROM LOAD_TEST_RND");
		process.addSource(source);

        JDBCExtractor processExtractor = new JDBCExtractor();
        processExtractor.setResultSetType(ResultSet.TYPE_FORWARD_ONLY);
		process.setExtractor(processExtractor);
		
		CSVLoader csvLoader = new CSVLoader();
		csvLoader.setFilename("test/jdbc_extract.csv");
		
		process.addLoader(csvLoader);
		
		pc.start();

	}
	
	
}
