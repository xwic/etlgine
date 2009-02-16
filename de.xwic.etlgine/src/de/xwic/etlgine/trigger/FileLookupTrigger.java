/**
 * 
 */
package de.xwic.etlgine.trigger;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileLock;

import de.xwic.etlgine.ITrigger;

/**
 * The trigger is due when the specified file exists. Used to check a specified
 * file location.
 * @author lippisch
 */
public class FileLookupTrigger implements ITrigger {

	private File file;
	private long lastSize = -1;
	
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

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	public boolean isDue() {
		if (file.exists()) {
			try {
				FileOutputStream fos = new FileOutputStream(file, true);
				
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
				
				long size = file.length();
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
				return false;
			}
		}
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
