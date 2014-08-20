/**
 * Setup the TestRnd2JDBCprocess.
 */
 
import de.xwic.etlgine.*
import de.xwic.etlgine.demo.DemoRndExtractor
import de.xwic.etlgine.demo.DemoRndSource
import de.xwic.etlgine.loader.jdbc.JDBCLoader
import de.xwic.etlgine.loader.jdbc.SqlDialect
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat

import de.xwic.etlgine.finalizer.PublishDataPoolsFinalizer

def source = new DemoRndSource(500);

process.addSource(source);

process.setExtractor(new DemoRndExtractor());

def jdbcLoader = new JDBCLoader();
jdbcLoader.setSharedConnectionName("defaultsqliteshare");
jdbcLoader.setConnectionName("defaultsqlite");
jdbcLoader.setTablename("LOAD_TEST_RND");
jdbcLoader.setAutoCreateColumns(true);
jdbcLoader.setSqlDialect(SqlDialect.SQLITE);

jdbcLoader.addIgnoreableColumns("ID");

process.addLoader(jdbcLoader);


// add a test transformer
class MyTransformer extends AbstractTransformer {
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
        SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		def obj = record.getData("Date");
		if (obj!= null) {
            record.setData("Date", dt.format(obj));
		}
	}
}
process.addTransformer(new MyTransformer());

process.addProcessFinalizer(new PublishDataPoolsFinalizer());
