/**
 * 
 */
package de.xwic.cube;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import junit.framework.TestCase;
import de.xwic.cube.formatter.PercentageValueFormatProvider;
import de.xwic.cube.functions.DifferenceFunction;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.cube.util.DataDump;

/**
 * @author Florian Lippisch
 */
public class CubeSpeedTest extends TestCase {

	ICube cube = null;
	private IMeasure meBookings;
	private IMeasure mePlan;
	private IMeasure meDiff;
	private IDimension dimOT;
	private IDimension dimLOB;
	private IDimension dimTime;
	private IDataPool pool;
	private IDataPoolManager manager;
	
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
		mePlan = pool.createMeasure("Plan");
		meDiff = pool.createMeasure("Diff");

		DifferenceFunction function = new DifferenceFunction(meBookings, mePlan, true); 
		meDiff.setFunction(function);
		meDiff.setValueFormatProvider(new PercentageValueFormatProvider());
		
		cube = pool.createCube("test", new IDimension[] { dimOT, dimLOB, dimTime }, new IMeasure[] { meBookings, mePlan, meDiff });
		
	}
	
	
	public void testSpeedWrite() {
		long l = System.currentTimeMillis();
		// write 1000 times.
		double d = 1.0;
		for (int i = 0; i < 100000; i++) {
			Key key = cube.createKey("[AOO][PS/Installation][2008/Q1]");
			cube.setCellValue(key, meBookings, d);
			d = d + 1.0d;
		}
		
		System.out.println("Time: " + (System.currentTimeMillis() - l));
		
	}
	
}
