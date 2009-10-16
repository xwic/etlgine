/**
 * 
 */
package de.xwic.cube;

import java.io.File;

import junit.framework.TestCase;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.cube.util.DataDump;

/**
 * @author Florian Lippisch
 */
public class DataPoolManagerTest extends TestCase {

	ICube cube = null;
	private IDataPoolManager manager;
	private IMeasure meBookings;
	private IDimension dimLOB;
	private IDimension dimOT;
	private IDimension dimTime;
	private IDataPool pool;

	@SuppressWarnings("unused")
	public void testCreateDataPool() {
		
		File dataDir = new File("data");
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(dataDir);
		IDataPoolManager manager = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		IDataPool pool = manager.createDataPool("test");
		assertNotNull(pool);
		assertEquals("test", pool.getKey());
		
		IDimension dimOT = pool.createDimension("OrderType");
		assertNotNull(dimOT);
		assertEquals("OrderType", dimOT.getKey());
		
		assertEquals(1, pool.getDimensions().size());
		
		IDimensionElement elmAOO = dimOT.createDimensionElement("AOO");	
		IDimensionElement elmCOO = dimOT.createDimensionElement("COO");
		
		assertEquals(2, dimOT.getDimensionElements().size());
		
		
		IDimension dimLOB = pool.createDimension("LOB");
		IDimensionElement elmHW = dimLOB.createDimensionElement("Hardware");
		IDimensionElement elmPS = dimLOB.createDimensionElement("PS");
		IDimensionElement elmInst = elmPS.createDimensionElement("Installation");
		IDimensionElement elmConsulting = elmPS.createDimensionElement("Consulting");
		elmPS.createDimensionElement("Service");
		IDimensionElement elmED = dimLOB.createDimensionElement("Education");
		 
		DataDump.printStructure(System.out, dimLOB);
		
		IMeasure meBookings = pool.createMeasure("Bookings");
		
		// now create a cube
		
		ICube cube = pool.createCube("test", new IDimension[] { dimOT, dimLOB }, new IMeasure[] { meBookings });
		assertNotNull(cube);
		
		Key key = new Key(new IDimensionElement[] { elmAOO, elmInst });
		cube.setCellValue(key, meBookings, 100.0d);
		
		key = new Key(new IDimensionElement[] { elmAOO, elmED });
		cube.setCellValue(key, meBookings, 200d);

		Double value = cube.getCellValue(key, meBookings);
		assertNotNull(value);
		assertEquals(200.0, value.doubleValue());
		
		key = new Key(new IDimensionElement[] { elmAOO, elmConsulting });
		cube.setCellValue(key, meBookings, 50d);

		
		key = new Key(new IDimensionElement[] { elmAOO, dimLOB });
		value = cube.getCellValue(key, meBookings);
		assertNotNull(value);
		assertEquals(350.0, value.doubleValue());
		
	}

	public void testSaveLoad() throws StorageException {
		
		long start = System.currentTimeMillis();
		Key key = cube.createKey("[AOO][Hardware][2008/Q1/Jan]");
		cube.setCellValue(key, meBookings, 100.0);
	
		key = cube.createKey("[AOO][Hardware][2008/Q1/Feb]");
		cube.setCellValue(key, meBookings, 40.0);
	
		key = cube.createKey("[AOO][Hardware][2008/Q1/Mar]");
		cube.setCellValue(key, meBookings, 80.0);
		
		key = cube.createKey("[AOO][PS/Consulting][2008/Q1/Feb]");
		cube.setCellValue(key, meBookings, 50.0);
	
		key = cube.createKey("[COO][PS/Consulting][2008/Q1/Mar]");
		cube.setCellValue(key, meBookings, 200.0);
		
		long duration = System.currentTimeMillis() - start;
		System.out.println("Duration: " + duration);
		
		System.out.println("Before Save");
		DataDump.printValues(System.out, cube, dimLOB, dimOT , meBookings);
	
		pool.save();
		
		manager.releaseDataPool(pool);	// release from memory
		
		// reload pool (into a new instance).
		IDataPool pool2 = manager.getDataPool(pool.getKey());
		assertNotNull(pool2);
		
		DataDump.printValues(System.out, 
				pool2.getCube(cube.getKey()), 
				pool2.getDimension(dimLOB.getKey()),
				pool2.getDimension(dimOT.getKey()), 
				pool2.getMeasure(meBookings.getKey()));
		
		assertEquals(220.0, cube.getCellValue("[AOO][Hardware][2008/Q1]", meBookings));
		assertEquals(220.0, cube.getCellValue("[*][Hardware]", meBookings));
		assertEquals(270.0, cube.getCellValue("[AOO][*]", meBookings));
		assertEquals(250.0, cube.getCellValue("[*][PS]", meBookings));
		assertEquals(250.0, cube.getCellValue("[LOB:PS]", meBookings));
		assertEquals(470.0, cube.getCellValue("", meBookings));
	
		
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		
		File dataDir = new File("data");
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(dataDir);
		manager = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		pool = manager.createDataPool("test");
		dimOT = pool.createDimension("OrderType");
		dimOT.createDimensionElement("AOO");	
		dimOT.createDimensionElement("COO");
		
		
		dimLOB = pool.createDimension("LOB");
		dimLOB.createDimensionElement("Hardware");
		IDimensionElement elmPS = dimLOB.createDimensionElement("PS");
		elmPS.createDimensionElement("Installation");
		elmPS.createDimensionElement("Consulting");
		elmPS.createDimensionElement("Service");
		dimLOB.createDimensionElement("Education");
		 
		
		dimTime = pool.createDimension("Time");
		IDimensionElement deY2008 = dimTime.createDimensionElement("2008");
		IDimensionElement deQ1 = deY2008.createDimensionElement("Q1");
		IDimensionElement deQ2 = deY2008.createDimensionElement("Q2");
		deQ1.createDimensionElement("Jan");
		deQ1.createDimensionElement("Feb");
		deQ1.createDimensionElement("Mar");
	
		deQ2.createDimensionElement("Apr");
		deQ2.createDimensionElement("May");
		deQ2.createDimensionElement("Jun");
		
		meBookings = pool.createMeasure("Bookings");
		cube = pool.createCube("test", new IDimension[] { dimOT, dimLOB, dimTime }, new IMeasure[] { meBookings });
		
	}
	
}
