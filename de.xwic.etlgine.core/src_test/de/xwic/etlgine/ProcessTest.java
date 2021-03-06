/*
 * de.xwic.etlgine.ProcessTest 
 */
package de.xwic.etlgine;

import java.io.FileReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;

import junit.framework.TestCase;
import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.csv.CSVLoader;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 */
public class ProcessTest extends TestCase {

	private Set<String> flags = new HashSet<String>();

	/**
	 * Test if all conditions are checked.
	 */
	public void testStartChecks() {
		
		IProcessChain pc = ETLgine.createProcessChain("Test");
		IETLProcess process = pc.createProcess("testStartChecks");
		assertNotNull(process);
		try {
			pc.start();
			fail("No exception alert");
		} catch (ETLException e) {
			// expected behaivior
		}
		
		FileSource srcFile = new FileSource("test/source.csv");
		process.addSource(srcFile);
		try {
			pc.start();
			fail("No exception and no loader defined");
		} catch (ETLException e) {
			// expected behaivior. -> should not start without a loader
		}
		
	}
	
	public void testProcess() throws ETLException {
		
		IProcessChain pc = ETLgine.createProcessChain("Test");
		final IETLProcess process = pc.createProcess("testStartChecks");
		
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
			public void onEvent(IProcessContext processContext, EventType eventType) {
                // Remove super call - else will end in a infinite loop
				//super.onEvent(processContext, eventType, process);
				switch (eventType) {
				case SOURCE_POST_OPEN: {
					// after the open, check if the data source contains the right columns.
					IDataSet dataSet = processContext.getDataSet();
					assertNotNull(dataSet);
					assertEquals(5, dataSet.getColumns().size());
					assertTrue(dataSet.containsColumn("Name"));
					for (IColumn col : dataSet.getColumns()) {
						System.out.println(col.getName());
					}
					break;
				}
				case SOURCE_FINISHED: {
					assertEquals(3, processContext.getRecordsCount());
					break;
				}
				}
			}
		});
		
		process.start();
		
	}
	
	public void testTransformerAddColumn() throws Exception {
		
		IProcessChain pc = ETLgine.createProcessChain("Test");
		IETLProcess process = pc.createProcess("testStartChecks");
		
		FileSource srcFile = new FileSource("test/source.csv");
		process.addSource(srcFile);
		assertEquals(1, process.getSources().size());
		
		CSVExtractor csvExtractor = new CSVExtractor();
		csvExtractor.setSeparator('\t');
		process.setExtractor(csvExtractor);

		// add CSV writer
		CSVLoader loader = new CSVLoader();
		loader.setFilename("test/export2.csv");
		loader.setSeparator(';');
		process.addLoader(loader);

		process.addTransformer(new AbstractTransformer() {
			IColumn colTest;
			/* (non-Javadoc)
			 * @see de.xwic.etlgine.impl.AbstractTransformer#preSourceProcessing(de.xwic.etlgine.IETLContext)
			 */
			@Override
			public void preSourceProcessing(IProcessContext processContext) throws ETLException {
				colTest = processContext.getDataSet().addColumn("Test");
				flags.add("preSourceProcessing");
			}
			/* (non-Javadoc)
			 * @see de.xwic.etlgine.impl.AbstractTransformer#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
			 */
			@Override
			public void processRecord(IProcessContext processContext, IRecord record) {
				record.setData(colTest, new Date());
				flags.add("processRecord");
			}
		});
		
		process.start();
		
		if (!flags.contains("preSourceProcessing")) {
			fail("ITransformer.preSourceProcessing not executed.");
		}
		if (!flags.contains("processRecord")) {
			fail("ITransformer.processRecord not executed.");
		}
		
		// check result
		CSVReader reader = new CSVReader(new FileReader("test/export2.csv"), ';', '"');
		String[] header = reader.readNext();
		assertNotNull(header);
		assertEquals(6, header.length);
		assertEquals("ID", header[0]);
		assertEquals("Test", header[5]);
		reader.close();
	}

	public void testTransformerHideColumn() throws Exception {
		
		IProcessChain pc = ETLgine.createProcessChain("Test");
		IETLProcess process = pc.createProcess("testStartChecks");
			
		FileSource srcFile = new FileSource("test/source.csv");
		process.addSource(srcFile);
		assertEquals(1, process.getSources().size());
		
		CSVExtractor csvExtractor = new CSVExtractor();
		csvExtractor.setSeparator('\t');
		process.setExtractor(csvExtractor);

		// add CSV writer
		CSVLoader loader = new CSVLoader();
		loader.setFilename("test/export3.csv");
		loader.setSeparator(';');
		process.addLoader(loader);

		process.addTransformer(new AbstractTransformer() {
			/* (non-Javadoc)
			 * @see de.xwic.etlgine.impl.AbstractTransformer#preSourceProcessing(de.xwic.etlgine.IETLContext)
			 */
			@Override
			public void preSourceProcessing(IProcessContext processContext) throws ETLException {
				IColumn col = processContext.getDataSet().getColumn("ID");
				col.setExclude(true);
			}
		});
		
		process.start();
		
		// check result
		CSVReader reader = new CSVReader(new FileReader("test/export3.csv"), ';', '"');
		String[] header = reader.readNext();
		assertNotNull(header);
		assertEquals(4, header.length);
		assertEquals("gebdate", header[0]);
		assertEquals("Area", header[3]);
		reader.close();
	}

	
}
