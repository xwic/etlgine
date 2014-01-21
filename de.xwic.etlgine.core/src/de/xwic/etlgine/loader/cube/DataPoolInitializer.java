/*
 * de.xwic.etlgine.loader.cube.DataPoolInitializer 
 */
package de.xwic.etlgine.loader.cube;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.IOException;

import org.codehaus.groovy.control.CompilationFailedException;

import de.xwic.cube.IDataPool;
import de.xwic.etlgine.IContext;

/**
 * Initializes and verifies the DataPool
 * @author lippisch
 */
public class DataPoolInitializer {

	private File scriptFile;
	private final IContext context;

	/**
	 * @param scriptFile
	 */
	public DataPoolInitializer(IContext context, File scriptFile) {
		super();
		this.context = context;
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
		binding.setVariable("util", new DataPoolInitializerUtil(pool, context));
		binding.setVariable("context", context);

		GroovyShell shell = new GroovyShell(binding);
		shell.evaluate(scriptFile);
		
	}

}
