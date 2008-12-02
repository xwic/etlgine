/*
 * de.xwic.etlgine.ProcessTest 
 */
package de.xwic.etlgine;

import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.CSVLoader;
import de.xwic.etlgine.sources.FileSource;
import junit.framework.TestCase;

/**
 * @author lippisch
 */
public class ProcessTest extends TestCase {


	/**
	 * Test if all conditions are checked.
	 */
	public void testStartChecks() {
		
		IETLProcess process = ETLgine.createETLProcess();
		assertNotNull(process);
		try {
			process.start();
			fail("No exception alert");
		} catch (ETLException e) {
			// expected behaivior
		}
		
		FileSource srcFile = new FileSource("test/source.csv");
		process.addSource(srcFile);
		try {
			process.start();
			fail("No exception and no loader defined");
		} catch (ETLException e) {
			// expected behaivior. -> should not start without a loader
		}
		
	}
	
	public void testProcess() throws ETLException {
		
		IETLProcess process = ETLgine.createETLProcess();
		
		FileSource srcFile = new FileSource("test/source.csv");
		process.addSource(srcFile);
		assertEquals(1, process.getSources().size());
		
		CSVExtractor csvExtractor = new CSVExtractor();
		csvExtractor.setSeparator('\t');
		process.setExtractor(csvExtractor);

		// add CSV writer
		CSVLoader loader = new CSVLoader();
		loader.setFilename("test/export.csv");
		loader.setSeparator(';');
		process.addLoader(loader);
		
		// install monitor that does the various tests.
		process.setMonitor(new DefaultMonitor() {
			@Override
			public void onEvent(IETLContext context, EventType eventType) {
				switch (eventType) {
				case SOURCE_POST_OPEN: {
					// after the open, check if the data source contains the right columns.
					IDataSet dataSet = context.getDataSet();
					assertNotNull(dataSet);
					assertEquals(5, dataSet.getColumns().size());
					assertTrue(dataSet.containsColumn("Name"));
					for (IColumn col : dataSet.getColumns()) {
						System.out.println(col.getName());
					}
					break;
				}
				case SOURCE_FINISHED: {
					assertEquals(3, context.getRecordsProcessed());
					break;
				}
				}
			}
		});
		
		process.start();
		
	}
	
}
