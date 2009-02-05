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
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created on Feb 5, 2009
 * @author JBORNEMA
 */

public class ZipEntrySource extends FileSource {

	protected ZipFile zipFile;
	protected ZipEntry zipEntry;
	
	/**
	 * 
	 */
	public ZipEntrySource(ZipFile zipFile, ZipEntry zipEntry) {
		this.zipFile = zipFile;
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
		return super.getName();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getFile()
	 */
	@Override
	public File getFile() {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getInputStream()
	 */
	@Override
	public InputStream getInputStream() throws IOException {
		return zipFile.getInputStream(zipEntry);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#isAvailable()
	 */
	@Override
	public boolean isAvailable() {
		return zipFile != null && zipEntry != null && !zipEntry.isDirectory();
	}
}
