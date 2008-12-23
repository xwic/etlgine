/**
 * 
 */
package de.xwic.etlgine;

import java.io.File;

import de.xwic.cube.DataPoolManagerFactory;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.IDataPoolStorageProvider;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.cube.CubeLoader;
import de.xwic.etlgine.loader.cube.DataPoolInitializer;
import de.xwic.etlgine.loader.cube.IDataPoolProvider;
import de.xwic.etlgine.loader.cube.ScriptedCubeDataMapper;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;
import de.xwic.etlgine.sources.FileSource;
import junit.framework.TestCase;

/**
 * @author Developer
 *
 */
public class JDBCTest extends TestCase {

	/**
	 * Test the loading of source data into a JDBC table.
	 * @throws ETLException 
	 */
	public void testJDBCLoader() throws ETLException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");
		IProcess process = pc.createProcess("jdbcLoad");
		
		/**
		 * Define Source
		 */
		FileSource srcFile = new FileSource("test/source_cube.csv");
		process.addSource(srcFile);
		assertEquals(1, process.getSources().size());
		
		CSVExtractor csvExtractor = new CSVExtractor();
		csvExtractor.setSeparator('\t');
		process.setExtractor(csvExtractor);
		

		JDBCLoader jdbcLoader = new JDBCLoader();
		jdbcLoader.setCatalogName("etlgine_test");
		jdbcLoader.setConnectionUrl("jdbc:jtds:sqlserver://localhost/etlgine_test");
		jdbcLoader.setUsername("etlgine");
		jdbcLoader.setPassword("etl");
		jdbcLoader.setTablename("LOAD_TEST");
		
		
		process.addLoader(jdbcLoader);
		
		pc.start();

		//DataDump.printStructure(System.out, pool.getDimension("Area"));

		
	}
	
}
