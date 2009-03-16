/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.loader.cube.DataPoolInitializerUtil.java
 * Created on Jan 30, 2009
 * 
 * @author jbornema
 */
package de.xwic.etlgine.loader.cube;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import de.xwic.cube.ICube;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.util.JDBCSerializerUtil;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * Utility class for the initializer scripts.
 * @author lippisch
 */
public class DataPoolInitializerUtil {
	
	private final static String[] MONTH_KEYS = { "Jan", "Feb", "Mar", "Apr",
		"May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

	private final IContext context;
	private IDataPool pool;
	
	/**
	 * Default constructor
	 * @param pool
	 * @param context
	 */
	public DataPoolInitializerUtil(IDataPool pool, IContext context) {
		super();
		this.pool = pool;
		this.context = context;
	}
	/**
	 * Ensure that a specified dimension exists.
	 * @param key
	 * @return
	 */
	public IDimension ensureDimension(String key) {
		if (!pool.containsDimension(key)) {
			return pool.createDimension(key) ;
		}
		return pool.getDimension(key);
	}
	/**
	 * Ensure that a specified dimension exists.
	 * @param key
	 * @return
	 */
	public IDimension ensureDimension(String key, String title) {
		if (!pool.containsDimension(key)) {
			IDimension dim = pool.createDimension(key) ;
			dim.setTitle(title);
			return dim;
		}
		return pool.getDimension(key);
	}
	/**
	 * Ensure that the specified measure exists.
	 * @param key
	 * @return
	 */
	public IMeasure ensureMeasure(String key) {
		if (!pool.containsMeasure(key)) {
			return pool.createMeasure(key) ;
		}
		return pool.getMeasure(key);
	}
	/**
	 * Ensure that the specified measure exists.
	 * @param key
	 * @return
	 */
	public IMeasure ensureMeasure(String key, String title) {
		if (!pool.containsMeasure(key)) {
			IMeasure measure = pool.createMeasure(key) ;
			measure.setTitle(title);
			return measure;
		}
		return pool.getMeasure(key);
	}
	
	/**
	 * Ensure that the specified dimension element exists.
	 * @param dimKey
	 * @param elmPath
	 * @return
	 */
	public IDimensionElement ensureElement(String dimKey, String elmPath) {
		IDimension dim = pool.getDimension(dimKey);
		String[] path = elmPath.split("/");
		IDimensionElement elm = dim;
		for (String s : path) {
			if (elm.containsDimensionElement(s)) {
				elm = elm.getDimensionElement(s);
			} else {
				elm = elm.createDimensionElement(s);
			}
		}
		return elm;
	}
	/**
	 * Creates a time hierachy with Year/Quater/Month.
	 * 
	 * @param timeDimension
	 * @param year
	 * @param firstMonth
	 */
	public void ensureTimeElements(IDimension timeDimension, int year, int firstMonth) {

		String keyYear = Integer.toString(year);
		IDimensionElement deYear = timeDimension
				.containsDimensionElement(keyYear) ? timeDimension
				.getDimensionElement(keyYear) : timeDimension
				.createDimensionElement(keyYear);

		int month = firstMonth;
		for (int i = 1; i < 5; i++) {
			String keyQ = "Q" + i;
			IDimensionElement deQ = deYear.containsDimensionElement(keyQ) ? deYear
					.getDimensionElement(keyQ)
					: deYear.createDimensionElement(keyQ);
			for (int m = 0; m < 3; m++) {
				if (!deQ.containsDimensionElement(MONTH_KEYS[month])) {
					deQ.createDimensionElement(MONTH_KEYS[month]);
				}
				month++;
				if (month >= MONTH_KEYS.length) {
					month = 0;
				}
			}
		}

	}

	/**
	 * Ensure that the specified cube exists. If it does not exist, a cube of type DEFAULT is created.
	 * @param key
	 * @param dimKeys
	 * @param measureKeys
	 * @return
	 */
	public ICube ensureCube(String key, List<String> dimKeys, List<String> measureKeys) {
		return ensureCube(key, dimKeys, measureKeys, IDataPool.CubeType.DEFAULT);
	}
	
	/**
	 * Ensure that the specified cube exists. If it does not exists, it is created with the 
	 * specified cubeType setting.
	 * @param key
	 * @param dimKeys
	 * @param measureKeys
	 * @param cubeType
	 * @return
	 */
	public ICube ensureCube(String key, List<String> dimKeys, List<String> measureKeys, IDataPool.CubeType cubeType) {
		if (!pool.containsCube(key)) {
			IDimension[] dimensions = new IDimension[dimKeys.size()];
			for (int i = 0; i < dimKeys.size(); i++) {
				dimensions[i] = pool.getDimension(dimKeys.get(i));
			}
			IMeasure[] measures = new IMeasure[measureKeys.size()];
			for (int i = 0; i < measureKeys.size(); i++) {
				measures[i] = pool.getMeasure(measureKeys.get(i));
			}
			pool.createCube(key, dimensions, measures, cubeType);
		}
		return pool.getCube(key);
	}
	
	public void initFromDatabase(String dbProfile) throws ETLException, SQLException {
		
		Connection connection = JDBCUtil.openConnection(context, dbProfile);
		try {
			JDBCSerializerUtil.restoreDimensions(connection, pool, "XCUBE_DIMENSIONS", "XCUBE_DIMENSION_ELEMENTS");
		} finally {
			connection.close();
		}
		
	}
	
}
