/**
 * Setup the TestRnd2JDBCprocess.
 */
 
import de.xwic.etlgine.*
import de.xwic.etlgine.loader.jdbc.JDBCLoader

def source = new TestRndSource(10000);

process.addSource(source);

process.setExtractor(new TestRndExtractor());

def jdbcLoader = new JDBCLoader();
jdbcLoader.setCatalogName("etlgine_test");
jdbcLoader.setConnectionUrl("jdbc:jtds:sqlserver://localhost/etlgine_test");
jdbcLoader.setUsername("etlgine");
jdbcLoader.setPassword("etl");
jdbcLoader.setTablename("LOAD_TEST_RND");
jdbcLoader.setAutoCreateColumns(true);

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

