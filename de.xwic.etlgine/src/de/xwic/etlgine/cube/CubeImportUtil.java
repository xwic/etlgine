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
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.Key;
import de.xwic.etlgine.ETLException;

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
	 * @throws ETLException 
	 */
	public static void importCSV(InputStream in, ICube cube) throws IOException, ETLException {
	
		new CubeImportUtil()._importCSV(in, cube);
		
	}
	
	/**
	 * Internal reader.
	 * @param in
	 * @param cube
	 * @throws IOException
	 * @throws ETLException 
	 */
	private void _importCSV(InputStream in, ICube cube) throws IOException, ETLException {
		
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
		
		// check if all dimensions in the cube are represented in the file
		
		for (IDimension dim : cube.getDimensions()) {
			if (!dimRef.containsKey(dim)) {
				throw new ETLException("The file does not contain the dimension '" + dim.getKey() + "', which is defined in the cube.");
			}
		}
		
		if (meRef.size() == 0) {
			throw new ETLException("The file does not contain any measure data that is defined by the cube.");
		}
		
		// start reading
		String[] data;
		Key key = cube.createKey("");
		
		while ((data = csvIn.readNext()) != null) {
			
			int keyIdx = 0;
			for (IDimension dim : cube.getDimensions()) {
				idx = dimRef.get(dim);
				String path = data[idx];
				
				IDimensionElement elm = dim.parsePath(path);
				key.setDimensionElement(keyIdx, elm);
				keyIdx++;
			}
			
			// read measures
			for (IMeasure measure : meRef.keySet()) {
				idx = meRef.get(measure);
				String value = data[idx];
				if (value.length() == 0) {
					// do nothing
				} else {
					double dblValue = Double.parseDouble(value);
					cube.addCellValue(key, measure, dblValue);
				}
			}
			
			
		}
		
	}
	
}
