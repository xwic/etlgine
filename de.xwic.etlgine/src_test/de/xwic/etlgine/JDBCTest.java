/**
 * 
 */
package de.xwic.etlgine;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import junit.framework.TestCase;
import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;
import de.xwic.etlgine.sources.FileSource;

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
		

		// add transformer that gives some "type" hints
		ITransformer colTrans = new AbstractTransformer() {
			 /* (non-Javadoc)
			 * @see de.xwic.etlgine.impl.AbstractTransformer#postSourceProcessing(de.xwic.etlgine.IContext)
			 */
			@Override
			public void preSourceProcessing(IContext context) throws ETLException {
				IDataSet ds = context.getDataSet();
				if (ds.containsColumn("gebdate")) {
					ds.getColumn("gebdate").setTypeHint(IColumn.DataType.DATE);
				}
				if (ds.containsColumn("Booking")) {
					ds.getColumn("Booking").setTypeHint(IColumn.DataType.DOUBLE);
				}	
				if (ds.containsColumn("ID")) {
					ds.getColumn("ID").setTargetName("SV_ID");
					ds.getColumn("ID").setTypeHint(IColumn.DataType.INT);
				}	
			}
			
			/* (non-Javadoc)
			 * @see de.xwic.etlgine.impl.AbstractTransformer#processRecord(de.xwic.etlgine.IContext, de.xwic.etlgine.IRecord)
			 */
			@Override
			public void processRecord(IContext context, IRecord record) throws ETLException {
				
				String sGebDate = record.getDataAsString("gebdate");
				if (sGebDate == null || sGebDate.trim().length() == 0) {
					record.setData("gebdate", null);
				} else {
					// try to parse
					try {
						DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.GERMAN);
						Date date = df.parse(sGebDate);
						record.setData("gebdate", date);
					} catch (ParseException pe) {
						record.markInvalid("Unparsable Date: " + sGebDate + " Exception: " + pe);
					}
				}
				
			}
			
		};
		
		process.addTransformer(colTrans);
		
		JDBCLoader jdbcLoader = new JDBCLoader();
		jdbcLoader.setCatalogName("etlgine_test");
		jdbcLoader.setConnectionUrl("jdbc:jtds:sqlserver://localhost/etlgine_test");
		jdbcLoader.setUsername("etlgine");
		jdbcLoader.setPassword("etl");
		jdbcLoader.setTablename("LOAD_TEST");
		jdbcLoader.setAutoCreateColumns(true);
		
		
		
		process.addLoader(jdbcLoader);
		
		pc.start();

		//DataDump.printStructure(System.out, pool.getDimension("Area"));

		
	}
	
}
