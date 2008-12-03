/*
 * de.xwic.etlgine.CubeLoaderTest 
 */
package de.xwic.etlgine;

import java.io.File;

import junit.framework.TestCase;
import de.xwic.cube.DataPoolManagerFactory;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.IDataPoolStorageProvider;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.cube.util.DataDump;
import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.cube.CubeLoader;
import de.xwic.etlgine.loader.cube.DataPoolInitializer;
import de.xwic.etlgine.loader.cube.IDataPoolProvider;
import de.xwic.etlgine.loader.cube.ScriptedCubeDataMapper;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 */
public class CubeLoaderTest extends TestCase {

	public void testCubeLoader() throws ETLException {
		
		IETLProcess process = ETLgine.createETLProcess("cubeLoadeTest");
		FileSource srcFile = new FileSource("test/source_cube.csv");
		process.addSource(srcFile);
		assertEquals(1, process.getSources().size());
		
		CSVExtractor csvExtractor = new CSVExtractor();
		csvExtractor.setSeparator('\t');
		process.setExtractor(csvExtractor);

		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(new File("test"));
		IDataPoolManager dpm = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		final IDataPool pool = dpm.createDataPool("Test");
		
		// add cube loader
		IDataPoolProvider dpp = new IDataPoolProvider() {
			/* (non-Javadoc)
			 * @see de.xwic.etlgine.loader.cube.IDataPoolProvider#getDataPool()
			 */
			public IDataPool getDataPool() {
				return pool;
			}
		};
		CubeLoader cubeLoader = new CubeLoader(dpp);
		cubeLoader.setTargetCubeKey("Test");
		cubeLoader.setDataPoolInitializer(new DataPoolInitializer(new File("scripts/testcube.init.groovy")));
		cubeLoader.setDataMapper(new ScriptedCubeDataMapper(new File("scripts/testcube.mapping.groovy")));
		process.addLoader(cubeLoader);

		process.start();

		//DataDump.printStructure(System.out, pool.getDimension("Area"));
		DataDump.printValues(System.out, pool.getCube("Test"), pool.getDimension("Area"), pool.getDimension("Name"), pool.getMeasure("Bookings"));
	}
	
}
