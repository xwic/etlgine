/**
 * 
 */
package de.xwic.etlgine.trigger;

import java.io.File;
import java.io.FileOutputStream;

import de.xwic.etlgine.ITrigger;

/**
 * The trigger is due when the specified file exists. Used to check a specified
 * file location.
 * @author lippisch
 */
public class FileLookupTrigger implements ITrigger {

	private File file;
	
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
				fos.close();
				return true;
			} catch (Throwable t) {
				// the file is not yet ready to be opened -> most probably still being
				// written 
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
