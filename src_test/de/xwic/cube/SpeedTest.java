/**
 * 
 */
package de.xwic.cube;

import java.io.File;

import junit.framework.TestCase;
import de.xwic.cube.formatter.PercentageValueFormatProvider;
import de.xwic.cube.functions.DifferenceFunction;
import de.xwic.cube.impl.CubeFlexCalc;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.cube.util.CubeUtil;
import de.xwic.cube.util.DataDump;

/**
 * @author Florian Lippisch
 */
public class SpeedTest extends TestCase {

	private ICube cubeDefault = null;
	private ICube cubeFlex = null;
	private IMeasure meBookings;
	private IMeasure mePlan;
	private IMeasure meDiff;
	private IDimension dimOT;
	private IDimension dimGEO;
	private IDimension dimAccount;
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
		dimLOB.createDimensionElement("Licences");
		dimLOB.createDimensionElement("Software");
		 
		
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
		
		dimGEO = pool.createDimension("GEO");
		dimAccount = pool.createDimension("Account");
		
		CubeUtil util = new CubeUtil();
		util.setAutoCreateDimensionElement(true);
		util.setDataPool(pool);
		util.parseDimensionElementId("[GEO:EMEA/Germany]");
		util.parseDimensionElementId("[GEO:EMEA/UK]");
		util.parseDimensionElementId("[GEO:EMEA/France]");
		util.parseDimensionElementId("[GEO:EMEA/SNEE/NEE/Nordics]");
		util.parseDimensionElementId("[GEO:EMEA/SNEE/eMed/SA]");
		util.parseDimensionElementId("[GEO:America/This/Is/Depth1]");
		util.parseDimensionElementId("[GEO:America/This/Is/Depth2]");
		util.parseDimensionElementId("[GEO:America/This/Is/Depth3]");
		util.parseDimensionElementId("[Account:TEA/BT]");
		util.parseDimensionElementId("[Account:TEA/BA]");
		util.parseDimensionElementId("[Account:TEA/DT]");
		util.parseDimensionElementId("[Account:TEA/SAP]");
		util.parseDimensionElementId("[Account:TEA/NOKIA]");
		util.parseDimensionElementId("[Account:EA/RBS]");
		util.parseDimensionElementId("[Account:EA/BMW]");
		util.parseDimensionElementId("[Account:EA/XYZ]");
		util.parseDimensionElementId("[Account:EA/ABC]");
		util.parseDimensionElementId("[Account:EA/Test]");
		util.parseDimensionElementId("[Account:EA/Bla Bla]");
		for (int i = 0; i < 50; i++) {
			util.parseDimensionElementId("[Account:Territory/Test" + i + "]");
		}
		

		DifferenceFunction function = new DifferenceFunction(meBookings, mePlan, true); 
		meDiff.setFunction(function);
		meDiff.setValueFormatProvider(new PercentageValueFormatProvider());
		
		cubeDefault = pool.createCube("testDef", new IDimension[] { dimOT, dimLOB, dimTime, dimGEO, dimAccount }, new IMeasure[] { meBookings, mePlan, meDiff }, IDataPool.CubeType.DEFAULT);
		cubeFlex = pool.createCube("testFlex", new IDimension[] { dimAccount, dimLOB, dimOT, dimTime, dimGEO, }, new IMeasure[] { meBookings, mePlan, meDiff }, IDataPool.CubeType.FLEX_CALC);
		
	}
	
	public void testSpeed() {

		long duration = 0;
//		long duration = runSpeedTest(cubeDefault);
//		System.out.println("Duration (DEFAULT): " + duration);
//		System.out.println("Cube Size: " + cubeDefault.getSize());

		long durationFlex = runSpeedTest(cubeFlex);
		System.out.println("Duration (FLEX): " + durationFlex);
		System.out.println("Cube Size: " + cubeFlex.getSize());

		
		System.out.println("Duration (FLEX): " + durationFlex + " vs. DEFAULT = " + duration);
		
		/*
		Result
		Write; 	Total;
		78		4578		// starting values
		109		4594		// added rootIndex
		140		437			// changed dimension order (now starts with Account)
		203		1594		// changed dimension order (now starts with LOB);
		
		 */

		((CubeFlexCalc)cubeFlex).refreshCache();
		for (int i = 0; i < 50; i++) { 
			cubeFlex.getCellValue("[Account:Territory][LOB:Software]", meBookings);
		}
		DataDump.printValues(System.out, cubeFlex, dimTime, dimLOB, meBookings);
		((CubeFlexCalc)cubeFlex).refreshCache();
		DataDump.printValues(System.out, cubeFlex, dimGEO, dimAccount, meBookings);
		((CubeFlexCalc)cubeFlex).refreshCache();
		
		((CubeFlexCalc)cubeFlex).printStats(System.out);

		((CubeFlexCalc)cubeFlex).printCacheProfile(System.out);

	}
	
	private long runSpeedTest(ICube cube) {
		long start = System.currentTimeMillis();
		
		cube.beginMassUpdate();
		
		// splash from the top -> make sure all cell's are written.
		cube.setCellValue(cube.createKey(), meBookings, 10000);
		
		Key key = cube.createKey("[OrderType:AOO][LOB:Hardware][Time:2008/Q1/Jan]");
		cube.setCellValue(key, meBookings, 100.0);

		key = cube.createKey("[OrderType:AOO][LOB:Hardware][Time:2008/Q1/Feb]");
		cube.setCellValue(key, meBookings, 40.0);

		key = cube.createKey("[OrderType:AOO][LOB:Hardware][Time:2008/Q1/Mar]");
		cube.setCellValue(key, meBookings, 80.0);
		
		key = cube.createKey("[OrderType:AOO][LOB:PS/Consulting][Time:2008/Q1/Feb]");
		cube.setCellValue(key, meBookings, 50.0);

		key = cube.createKey("[OrderType:COO][LOB:PS/Consulting][Time:2008/Q1/Mar]");
		cube.setCellValue(key, meBookings, 200.0);
		
		cube.massUpdateFinished();
		
		System.out.println("write perf: " + (System.currentTimeMillis() - start));
		
		DataDump.printValues(System.out, cube, dimAccount, dimLOB, meBookings);

		long duration = System.currentTimeMillis() - start;
		
		return duration;
	}
	
	public void xtestBigCube() throws StorageException {
		
		File dataDir = new File("..\\NGS Dashboard\\WEB-INF\\xcube");
		assertTrue(dataDir.exists());
		
		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(dataDir);
		IDataPoolManager manager = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		
		assertTrue(manager.containsDataPool("stoaud"));
		
		IDataPool pool = manager.getDataPool("stoaud");
		
		ICube cube = pool.getCube("stoaud");
		IMeasure measure = cube.getMeasures().iterator().next();
		
		
		assertEquals(CubeFlexCalc.class, cube.getClass());
		
		System.out.println("Start read ALL");
		long start = System.currentTimeMillis();
		
		Double value = cube.getCellValue("", measure);
		long duration = System.currentTimeMillis()- start;
		System.out.println("Duration 1: " + duration + ", result= " + value);
		
		System.out.println("Second Try:");
		start = System.currentTimeMillis();
		
		value = cube.getCellValue("", measure);
		duration = System.currentTimeMillis() - start;
		System.out.println("Duration 2: " + duration + ", result= " + value);

		
	}
	
}
