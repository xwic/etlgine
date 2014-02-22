/**
 * Setup the TestRnd2JDBCprocess.
 */
 
 
import de.xwic.etlgine.*
import de.xwic.etlgine.loader.jdbc.JDBCLoader
import de.xwic.etlgine.loader.jdbc.SqlDialect

def source = new TestRndSource(10000);

process.addSource(source);

process.setExtractor(new TestRndExtractor());

def jdbcLoader = new JDBCLoader();
jdbcLoader.setDriverName("org.sqlite.JDBC");
jdbcLoader.setConnectionUrl("jdbc:sqlite:test/etlgine_test.db3");
jdbcLoader.setUsername("");
jdbcLoader.setPassword("");
jdbcLoader.setTablename("LOAD_TEST_RND");
jdbcLoader.setAutoCreateColumns(true);
jdbcLoader.setSqlDialect(SqlDialect.SQLITE);
jdbcLoader.addIgnoreableColumns("ID");

process.addLoader(jdbcLoader);


// add a test transformer
class MyTransformer extends AbstractTransformer {
	public void processRecord(IContext context, IRecord record) throws ETLException {
		def obj = record.getDataAsDouble("Bookings");
		if (obj != null) {
			record.setData("Bookings", -obj);
		}
	}
}
process.addTransformer(new MyTransformer());

