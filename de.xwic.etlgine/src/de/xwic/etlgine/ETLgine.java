/*
 * de.xwic.etlgine.ETLgine 
 */
package de.xwic.etlgine;

import de.xwic.etlgine.impl.ProcessChain;

/**
 * 
 * @author lippisch
 */
public class ETLgine {

	/**
	 * Create a new ProcessChain.
	 * @return
	 */
	public static IProcessChain createProcessChain(String name) {
		return new ProcessChain(name);
	}
	
	/**
	 * Create a new ProcessChain.
	 * @return
	 */
	public static IProcessChain createProcessChain(IContext parentContext, String name) {
		return new ProcessChain(parentContext, name);
	}

}
