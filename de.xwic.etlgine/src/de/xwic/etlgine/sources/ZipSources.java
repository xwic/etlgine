/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.sources.ZipSources.java
 * Created on Feb 5, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.sources;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.impl.ETLProcess;

/**
 * Created on Feb 5, 2009
 * @author JBORNEMA
 */

public class ZipSources {

	protected File file;
	
	public ZipSources() {
		
	}
	
	public ZipSources(File file) throws ETLException {
		setFile(file);
	}
	
	public ZipSources(String filename) throws ETLException {
		setFile(new File(filename));
	}
	
	/**
	 * The file to set. 
	 * @param file
	 * @throws ETLException
	 */
	public void setFile(File file) throws ETLException {
		this.file = file;
	}
	
	/**
	 * @return the file
	 */
	public File getFile() {
		return file;
	}
	
	/**
	 * Adds all zip content as sources in process.
	 * @param process
	 * @throws ETLException 
	 */
	public void addSources(ETLProcess process) throws ETLException {
		try {
			ZipFile zipFile = new ZipFile(file);
			for (Enumeration<? extends ZipEntry> entries = zipFile.entries(); entries.hasMoreElements(); ) {
				ZipEntry entry = entries.nextElement();
				if (!entry.isDirectory()) {
					ZipEntrySource source = new ZipEntrySource(zipFile, entry);
					process.addSource(source);
				}
			}
		} catch (IOException ioe) {
			throw new ETLException(ioe);
		}
		
	}
}
