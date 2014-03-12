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
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.lang.StringUtils;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IProcessContext;

/**
 * Created on Feb 5, 2009
 * @author JBORNEMA
 */

public class ZipSources /*implements IProcessFinalizer*/ {

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
	 * Adds all zip content as sources in process and registers this as process finalizer as well.
	 * @param process
	 * @throws ETLException 
	 */
	public void addSources(IETLProcess process) throws ETLException {

		addSources(process, null);
	}
	
	/**
	 * Adds all zip content as sources in process when zipEntryRegEx is null or only the matching zip entries
	 * and registers this as process finalizer as well.
	 * @param process
	 * @param zipEntryRegEx
	 * @return true if one or more sources had been added
	 * @throws ETLException 
	 */
	public boolean addSources(IETLProcess process, String zipEntryRegEx) throws ETLException {
		
		ZipFile zipFile = null;
		
		try {
			if (file == null) {
				throw new ETLException("No file set!");
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
					if (i == 0 && StringUtils.isEmpty(filePrefix)) {
						file = locZFile;
					}
					//check file prefix, if found get out!
					if (filePrefix != null && locZFile.getName().toLowerCase().startsWith(filePrefix.toLowerCase())) {
						file = locZFile;
						break;
					}
				}
				
				if (file != null && file.exists() && file.isFile()) {
					zipFile = new ZipFile(file);
				}
			}
			
			if (zipFile == null) {
				throw new ETLException("Cannot find any valid ZIP Archive by given file/directory names! " + file.getAbsolutePath());
			}
			
			boolean added = false;
			Pattern zipEntryRegExPattern = null;
			if (zipEntryRegEx != null) {
				zipEntryRegExPattern = Pattern.compile(zipEntryRegEx);
			}
			for (Enumeration<? extends ZipEntry> entries = zipFile.entries(); entries.hasMoreElements(); ) {
				ZipEntry entry = entries.nextElement();
				if (!entry.isDirectory() && (zipEntryRegExPattern == null || zipEntryRegExPattern.matcher(entry.getName()).matches())) {
					ZipEntrySource source = new ZipEntrySource(this, entry);
					process.addSource(source);
					if (!added) {
						added = true;
						//process.addProcessFinalizer(this);
					}
				}
			}
			if (!added) {
				process.getMonitor().logInfo("Closing zip file " + zipFile.getName());
				close();
			}
			return added;
			
		} catch (IOException ioe) {
			
			throw new ETLException("Error processing file " + file, ioe);
			
		} finally {
			
			if (zipFile != null) {
				try {
					// close zip file
					zipFile.close();
					zipFile = null;
				} catch (Exception e) {}
			}
			
		}
	}
	
	/**
	 * Close file.
	 * @throws IOException 
	 */
	public void close() throws IOException {
		if (zipFile != null) {
			try {
				zipFile.close();
			} finally {
				zipFile = null;
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException {
		try {
			context.getMonitor().logInfo("Closing zip file " + zipFile.getName());
			close();
		} catch (IOException e1) {
			throw new ETLException("Error closing zip archive.", e1);
		}		
	}

	/**
	 * @return current ZipFile or creates a new one and sets zipFile field
	 * @throws IOException 
	 */
	public ZipFile getZipFile() throws IOException {
		if (zipFile == null) {
			zipFile = new ZipFile(file);
		}
		return zipFile;
	}
}
