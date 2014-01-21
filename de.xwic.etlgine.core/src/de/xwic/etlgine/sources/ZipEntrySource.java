/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.sources.ZipFileEntrySource.java
 * Created on Feb 5, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.sources;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created on Feb 5, 2009
 * @author JBORNEMA
 */

public class ZipEntrySource extends FileSource {

	protected ZipSources zipSource;
	protected ZipEntry zipEntry;
		/**
	 * @param zipSources 
	 * 
	 */
	public ZipEntrySource(ZipSources zipSource, ZipEntry zipEntry) {
		this.zipSource = zipSource;
		this.zipEntry = zipEntry;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getFilename()
	 */
	@Override
	public String getFilename() {
		return zipEntry.getName();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getName()
	 */
	@Override
	public String getName() {
		return zipEntry.getName();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getFile()
	 */
	@Override
	public File getFile() {
		// TODO check if logic is correct to return here the zip file archive...
		return zipSource.getFile();
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getInputStream()
	 */
	@Override
	public InputStream getInputStream() throws IOException {
		return new FilterInputStream(getZipFile().getInputStream(zipEntry)) {
			@Override
			public void close() throws IOException {
				super.close();
				// also close the zip archive
				zipSource.close();
			}
		};
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#isAvailable()
	 */
	@Override
	public boolean isAvailable() {
		try {
			return zipEntry != null && !zipEntry.isDirectory() && getZipFile() != null;
		} catch (IOException e) {
			// TODO check if it might be better to throw an exception than swallow it
			return false;
		}
	}
	
	/**
	 * @return ZipSources
	 * @throws IOException 
	 */
	public ZipSources getZipSources() throws IOException {
		return zipSource;
	}
	
	/**
	 * @return ZipFile
	 * @throws IOException 
	 */
	protected ZipFile getZipFile() throws IOException {
		return zipSource.getZipFile();
	}

	/**
	 * @deprecated use getZipFile()
	 * @return
	 * @throws IOException 
	 */
	public ZipFile getZipParent() throws IOException {
		return getZipFile();
	}
	
}