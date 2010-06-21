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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.impl.ETLProcess;

/**
 * Created on Feb 5, 2009
 * @author JBORNEMA
 */

public class ZipSources implements IProcessFinalizer {

	protected File file;
	private ZipFile zipFile = null;
	
	protected final static String ZIP_EXTENSION = ".zip";
	
	private String filePrefix = null;
	
	public ZipSources() {
		
	}
	
	/**
	 * Creates a zip source by given file name.
	 * If the file is a directory, the first zip file will be taken found in the directory.
	 * 
	 * @param file
	 * @throws ETLException
	 */
	public ZipSources(File file) throws ETLException {
		setFile(file);
	}

	/**
	 * Creates a zip source by given directory.
	 * If no file matches the given zipFilePrefix, then the first found zip file will be take.
	 * 
	 * @param directory
	 * @param zipFilePrefix
	 * @throws ETLException
	 */
	public ZipSources(File directory, String zipFilePrefix) throws ETLException {
		setFile(directory);
		this.filePrefix = zipFilePrefix;
	}
	
	/**
	 * Creates a zip source by given directory.
	 * If no file matches the given zipFilePrefix, then the first found zip file will be take.
	 * 
	 * @param directory
	 * @param zipFilePrefix
	 * @throws ETLException
	 */
	public ZipSources(String directory, String zipFilePrefix) throws ETLException {
		setFile(new File(directory));
		this.filePrefix = zipFilePrefix;
	}
	
	
	/**
	/**
	 * Creates a zip source by given file name.
	 * If the file is a directory, the first zip file will be taken found in the directory.
	 * 
	 * @param filename
	 * @throws ETLException
	 */
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
			if (file == null) {
				new ETLException("No file set!");
			}

			if (file.isFile()) {
				zipFile  = new ZipFile(file);
			} else if (file.isDirectory()) {
				//file is a directory, 
				File[] files = file.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.toLowerCase().endsWith(ZIP_EXTENSION);
					}
				});
				//check if the filename prefix is set and look for a file coming along
				for(int i = 0; i < files.length; i++) {
					File locZFile = files[i];
					
					//take first zip file in any case!!
					if (i == 0) {
						file = locZFile;
						zipFile = new ZipFile(locZFile); 
					}
					//check file prefix, if found get out!
					if (filePrefix != null && locZFile.getName().toLowerCase().startsWith(filePrefix.toLowerCase())) {
						file = locZFile;
						zipFile = new ZipFile(locZFile); 
						break;
					}
				}
			}
			
			if (zipFile == null) {
				new ETLException("Cannot find any valid ZIP Archive by given file/directory names! " + file.getAbsolutePath());
			}
			
			for (Enumeration<? extends ZipEntry> entries = zipFile.entries(); entries.hasMoreElements(); ) {
				ZipEntry entry = entries.nextElement();
				if (!entry.isDirectory()) {
					ZipEntrySource source = new ZipEntrySource(this, zipFile, entry);
					process.addSource(source);
				}
			}
		} catch (IOException ioe) {
			throw new ETLException(ioe);
		}
	}
	
	/**
	 * Close file.
	 * @throws IOException 
	 */
	public void close() throws IOException {
		zipFile.close();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException {
		try {
			close();
		} catch (IOException e1) {
			throw new ETLException("Error closing zip archive.", e1);
		}		
	}

}
