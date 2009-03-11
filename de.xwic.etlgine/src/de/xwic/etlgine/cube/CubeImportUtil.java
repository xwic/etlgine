/**
 * 
 */
package de.xwic.etlgine.cube;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.com.bytecode.opencsv.CSVReader;
import de.xwic.cube.ICube;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IMeasure;

/**
 * Import data from a CSV file into an ICube. There must be one column for each dimension
 * that is defined by the cube. The data is added to the cube, so if an element appears twice, the 
 * value is aggregated. Therefore it is a good idea to clear the cube before importing the data.
 * 
 * At least one measure needs to be there as well.
 * 
 * @author lippisch
 */
public class CubeImportUtil {

	private final Log log = LogFactory.getLog(CubeImportUtil.class);
	
	private CubeImportUtil() {
		
	}
	
	/**
	 * Import data from a CSV file into an ICube.
	 * @param in
	 * @param cube
	 * @throws IOException
	 */
	public static void importCSV(InputStream in, ICube cube) throws IOException {
	
		new CubeImportUtil()._importCSV(in, cube);
		
	}
	
	/**
	 * Internal reader.
	 * @param in
	 * @param cube
	 * @throws IOException
	 */
	private void _importCSV(InputStream in, ICube cube) throws IOException {
		
		CSVReader csvIn = new CSVReader(new BufferedReader(new InputStreamReader(in)));
		
		String[] header = csvIn.readNext();
		
		// validate that all dimensions are there and build a measure map.
		Map<IDimension, Integer> dimRef = new HashMap<IDimension, Integer>();
		Map<IMeasure, Integer> meRef = new HashMap<IMeasure, Integer>();
		
		IDataPool dataPool = cube.getDataPool();
		
		int idx = 0;
		for (String key : header) {
			// check if its a dimension
			if (dataPool.containsDimension(key)) {
				IDimension dim = dataPool.getDimension(key);
				if (!dimRef.containsKey(dim)) {
					dimRef.put(dim, idx);
				} else {
					log.warn("Duplicate dimension column: " + key + ", " + idx);
				}
			} else if (dataPool.containsMeasure(key)) {
				IMeasure measure = dataPool.getMeasure(key);
				if (!meRef.containsKey(measure)) {
					meRef.put(measure, idx);
				} else {
					log.warn("Duplicate measure column: " + key + ", " + idx);
				}
			} else {
				log.warn("Unknown column: [" + key + "] - the column will be ignored.");
			}
			idx++;
		}
		
	}
	
}
