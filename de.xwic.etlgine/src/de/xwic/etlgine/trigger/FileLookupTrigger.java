/**
 * 
 */
package de.xwic.etlgine.trigger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;

import de.xwic.etlgine.ITrigger;

/**
 * The trigger is due when the specified file exists. Used to check a specified
 * file location.
 * @author lippisch
 */
public class FileLookupTrigger implements ITrigger {

	private File file;
	private Map<String, Long> fmLastSize = new HashMap<String, Long>(); 
	private String prefix = null;
	private String suffix = null;
	
	private FilenameFilter fnFilter = new FilenameFilter() {
		/* (non-Javadoc)
		 * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
		 */
		public boolean accept(File dir, String name) {
			if (prefix != null && !name.toLowerCase().startsWith(prefix)) {
				return false;
			}
			if (suffix != null && !name.toLowerCase().endsWith(suffix)) {
				return false;
			}
			return true;
		}
	};
	
	/**
	 * @param file
	 */
	public FileLookupTrigger(File file) {
		super();
		this.file = file;
	}
	
	public FileLookupTrigger(String filePath) {
		this.file = new File(filePath);
	}

	/**
	 * Search in the specified directory for any file with the specified prefix and suffix.
	 * @param filePath
	 * @param prefix
	 * @param suffix
	 */
	public FileLookupTrigger(String filePath, String prefix, String suffix) {
		this.prefix = prefix.toLowerCase();
		this.suffix = suffix.toLowerCase();
		this.file = new File(filePath);
	}

	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	public boolean isDue() {
		if (file.isDirectory()) {
			File[] content = file.listFiles(fnFilter);
			for (File f : content) {
				if (testFile(f)) {
					return true;
				}
			}
		} else if (file.exists()) {
			return testFile(file);
		}
		return false;
	}

	/**
	 * @param testFile
	 * @return
	 */
	private boolean testFile(File testFile) {
		
		long lastSize = fmLastSize.containsKey(testFile.getName()) ? fmLastSize.get(testFile.getName()) : -1;  
		
		try {
			FileOutputStream fos = new FileOutputStream(testFile, true);
			
			// try to lock the file. If it fails, the file is still "in use"
			// and can not be processed.
			
			FileLock lock = fos.getChannel().tryLock();
			try {
				if (lock == null) {
					return false;
				}
				lock.release();
			} finally {
				fos.close();
			}
			
			long size = testFile.length();
			// if the file size has not increased since the last check, we can be
			// sure that the file is no longer "downloaded".
			// This fixes a problem with the NEO downloader who is not locking
			// the file.
			if (size == lastSize) {		
				return true;
			}
			lastSize = size;
		} catch (Throwable t) {
			// the file is not yet ready to be opened -> most probably still being
			// written 
			lastSize = -1;
		}
		
		fmLastSize.put(testFile.getName(), lastSize);
		
		return false;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	public void notifyJobFinished(boolean withErrors) {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobStarted()
	 */
	public void notifyJobStarted() {

	}

	/**
	 * @return the file
	 */
	public File getFile() {
		return file;
	}

}
