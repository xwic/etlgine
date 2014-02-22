/**
 * 
 */
package de.xwic.etlgine;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import de.xwic.etlgine.demo.DemoDatabaseUtil;
import de.xwic.etlgine.loader.jdbc.SqlDialect;
import junit.framework.TestCase;
import de.xwic.etlgine.extractor.CSVExtractor;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;
import de.xwic.etlgine.sources.FileSource;
import org.apache.log4j.BasicConfigurator;

/**
 * @author Developer
 *
 */
public class JDBCTest extends TestCase {

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
	 */
	public void testJDBCLoader() throws ETLException {
		
		IProcessChain pc = ETLgine.createProcessChain("testChain");
		IETLProcess process = pc.createProcess("jdbcLoad");
		
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
			public void preSourceProcessing(IProcessContext processContext) throws ETLException {
				IDataSet ds = processContext.getDataSet();
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
			public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
				
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
        jdbcLoader.setDriverName("org.sqlite.JDBC");
        jdbcLoader.setConnectionUrl("jdbc:sqlite:test/etlgine_test.db3");
        jdbcLoader.setUsername("");
        jdbcLoader.setPassword("");
		jdbcLoader.setTablename("LOAD_TEST");
		jdbcLoader.setAutoCreateColumns(true);
        jdbcLoader.setSqlDialect(SqlDialect.SQLITE);
		
		
		process.addLoader(jdbcLoader);
		
		pc.start();

		//DataDump.printStructure(System.out, pool.getDimension("Area"));
		
	}
}
