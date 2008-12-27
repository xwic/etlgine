/*
 * de.xwic.etlgine.loader.cube.ScriptedCubeDataMapper 
 */
package de.xwic.etlgine.loader.cube;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;

/**
 * This data mapper uses groovy script files to configure the mapping.
 * 
 * @author lippisch
 */
public class ScriptedCubeDataMapper extends AbstractCubeDataMapper {

	private final File scriptFile;

	public ScriptedCubeDataMapper(File scriptFile) {
		this.scriptFile = scriptFile;
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.loader.cube.AbstractCubeDataMapper#configure(de.xwic.etlgine.IETLContext)
	 */
	protected void configure(IProcessContext processContext) throws ETLException {
		
		Binding binding = new Binding();
		binding.setVariable("context", processContext);
		binding.setVariable("mapper", this);

		GroovyShell shell = new GroovyShell(binding);
		try {
			shell.evaluate(scriptFile);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + scriptFile.getName() + "':" + e, e);
		}
		
	}
	
}
