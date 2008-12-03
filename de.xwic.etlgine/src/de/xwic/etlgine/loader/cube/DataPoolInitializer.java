/*
 * de.xwic.etlgine.loader.cube.DataPoolInitializer 
 */
package de.xwic.etlgine.loader.cube;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.codehaus.groovy.control.CompilationFailedException;

import de.xwic.cube.ICube;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;

/**
 * Initializes and verifies the DataPool
 * @author lippisch
 */
public class DataPoolInitializer {

	private File scriptFile;

	/**
	 * Utility class for the initializer scripts.
	 * @author lippisch
	 */
	private class DataPoolInitializerUtil {
		
		private IDataPool pool;
		public DataPoolInitializerUtil(IDataPool pool) {
			super();
			this.pool = pool;
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
		 * Ensure that the specified dimemsion element exists.
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
		 * Ensure that the specified cube exists. If it does not exist, it is created.
		 * @param key
		 * @param dimKeys
		 * @param measureKeys
		 * @return
		 */
		public ICube ensureCube(String key, List<String> dimKeys, List<String> measureKeys) {
			if (!pool.containsCube(key)) {
				IDimension[] dimensions = new IDimension[dimKeys.size()];
				for (int i = 0; i < dimKeys.size(); i++) {
					dimensions[i] = pool.getDimension(dimKeys.get(i));
				}
				IMeasure[] measures = new IMeasure[measureKeys.size()];
				for (int i = 0; i < measureKeys.size(); i++) {
					measures[i] = pool.getMeasure(measureKeys.get(i));
				}
				pool.createCube(key, dimensions, measures);
			}
			return pool.getCube(key);
		}
	}
	
	/**
	 * @param scriptFile
	 */
	public DataPoolInitializer(File scriptFile) {
		super();
		this.scriptFile = scriptFile;
	}

	/**
	 * Run the verification script.
	 * @param pool
	 * @param scriptFile
	 * @throws CompilationFailedException
	 * @throws IOException
	 */
	public void verify(IDataPool pool) throws CompilationFailedException, IOException {
		
		Binding binding = new Binding();
		binding.setVariable("pool", pool);
		binding.setVariable("util", new DataPoolInitializerUtil(pool));

		GroovyShell shell = new GroovyShell(binding);
		shell.evaluate(scriptFile);
		
	}

}
