/**
 * 
 */
package de.xwic.cube;

import java.io.File;

import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.cube.util.DataDump;


/**
 * @author Florian Lippisch
 */
public class Demo {

	/**
	 * @param args
	 * @throws StorageException 
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) throws StorageException {
		
		File dataDir = new File("data");
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(dataDir);
		IDataPoolManager dataPoolManager = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		
		IDataPool dataPool = dataPoolManager.createDataPool("demo");
		
		IDimension dimGEO = dataPool.createDimension("GEO");
		IDimensionElement deEMEA = dimGEO.createDimensionElement("EMEA");
		IDimensionElement deGermany = deEMEA.createDimensionElement("Germany");
		IDimensionElement deEngland = deEMEA.createDimensionElement("England");
		IDimensionElement deBL = deEMEA.createDimensionElement("Benelux");
		IDimensionElement deNL = deBL.createDimensionElement("Netherland");
		deNL.setWeight(50);
		IDimensionElement deBelgium = deBL.createDimensionElement("Belgium");
		deBelgium.setWeight(30);
		IDimensionElement deLU = deBL.createDimensionElement("Luxemburg");
		deLU.setWeight(20);
		
		//DataDump.printStructure(System.out, dimGEO);
		
		IDimension dimLOB = dataPool.createDimension("LOB"); // Line Of Business
		dimLOB.createDimensionElement("Hardware");
		dimLOB.createDimensionElement("Software");
		IDimensionElement elmPS = dimLOB.createDimensionElement("PS");
		elmPS.createDimensionElement("Installation");
		elmPS.createDimensionElement("Consulting");
		elmPS.createDimensionElement("Service");
		dimLOB.createDimensionElement("Education");

		
		IMeasure mePLAN = dataPool.createMeasure("plan"); // Plan Umsatz
		IMeasure meIST = dataPool.createMeasure("ist"); // Ist Umsatz
		
		ICube cube = dataPool.createCube("demo", 
				new IDimension[] {
					dimGEO,
					dimLOB
				}
				, new IMeasure[] {
					mePLAN,
					meIST
				}
		
		);
		
		Key key = cube.createKey("[EMEA/Germany][Software]");
		cube.setCellValue(key, meIST, 200.0);
		key = cube.createKey("[EMEA/England][Software]");
		cube.setCellValue(key, meIST, 80.0);

		key = cube.createKey("[EMEA/England][Hardware]");
		cube.setCellValue(key, meIST, 50.0);
		
		DataDump.printValues(System.out, cube, dimGEO, dimLOB, meIST);
		
		key = cube.createKey("[LOB:Software][GEO:EMEA/Benelux]");
		cube.setCellValue(key, mePLAN, 120.0d);

		Double value = cube.getCellValue("[*][Software]", mePLAN);
		System.out.println(value);
		
		DataDump.printValues(System.out, cube, dimGEO, dimLOB, mePLAN);
		
		dataPool.save();
		
	}

}
