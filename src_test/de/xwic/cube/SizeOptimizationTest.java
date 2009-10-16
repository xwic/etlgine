/**
 * 
 */
package de.xwic.cube;

import java.io.File;

import junit.framework.TestCase;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;

/**
 * @author Florian Lippisch
 */
public class SizeOptimizationTest extends TestCase {

	public void testCreateAndSave() {
		
		File dataDir = new File("data");
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		IDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(dataDir);
		IDataPoolManager manager = DataPoolManagerFactory.createDataPoolManager(storageProvider);
		IDataPool pool = manager.createDataPool("sizetest");
		
		IMeasure measure = pool.createMeasure("m1");
		
		// create a cube with 5 dimensions, where each dimension has a depth of 2.
		
		int factor = 4;
		
		IDimension[] allDim = new IDimension[factor];
		for (int i = 0; i < factor; i++) {
			IDimension dim = pool.createDimension("dim" + i);
			allDim[i] = dim;
			for (int a = 0; a < factor; a++) {
				IDimensionElement elm = dim.createDimensionElement("child" + i + "." + a);
				for (int b = 0; b < factor; b++) {
					elm.createDimensionElement(elm.getKey() + "." + b);
				}
			}
		}
		
		ICube cube = pool.createCube("cube1", allDim, new IMeasure[] {measure});
		
		// write one value on top, to fill all cells.
		System.out.println("Start writing..");
		int cells = cube.setCellValue(cube.createKey(""), measure, 1000000.0d);
		System.out.println("Writing done, " + cells + " cells updated/created (total cells=" + cube.getSize() + ")");
		
		try {
			pool.save();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// as of starting
		// cells = 5,308,416 cells (total=194,481)
		// start size=17,901,292 bytes
		// 2nd   size=17,706,798 bytes ; 91per cell; (12.861 sec)
		// 3rd   size=13,817,225 bytes ; 71per cell; (8.875 sec)
		// 4th   size=12,844,790 bytes ; 66per cell; (8.719 sec)
		// 5th   size=11,289,259 bytes ; 58per cell; (4.019 sec)   -- cell use externalizable
		// 6th   size=8,177,352 bytes  ; 42per cell; (3.453 sec)   -- cube uses externalizable
		
		
		File file = new File("data/sizetest.datapool");
		if (file.exists()) {
			System.out.println("Size: " + file.length() + " ( " + (file.length() / cube.getSize()) + " per cell)");
		}
		
	}
}
