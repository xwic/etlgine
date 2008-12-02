/*
 * de.xwic.etlgine.ETLgine 
 */
package de.xwic.etlgine;

import de.xwic.etlgine.impl.ETLProcess;

/**
 * 
 * @author lippisch
 */
public class ETLgine {

	/**
	 * Create a new EtlProcess.
	 * @return
	 */
	public static IETLProcess createETLProcess() {
		return new ETLProcess();
	}
	
}
