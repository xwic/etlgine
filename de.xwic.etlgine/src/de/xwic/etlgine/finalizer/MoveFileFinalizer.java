/**
 * 
 */
package de.xwic.etlgine.finalizer;

import java.io.File;

import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 *
 */
public class MoveFileFinalizer implements IProcessFinalizer {

	private File targetPath;
	
	private boolean deleteTargetIfExists = true;
	
	/**
	 * @param targetPath
	 */
	public MoveFileFinalizer(File targetPath) {
		super();
		this.targetPath = targetPath;
	}

	/**
	 * @param targetPath
	 */
	public MoveFileFinalizer(String targetPath) {
		super();
		this.targetPath = new File(targetPath);
	}


	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) {
		
		IMonitor monitor = context.getMonitor();
		if (!targetPath.exists()) {
			if (!targetPath.mkdirs()) {
				monitor.logError("Error creating target directory: " + targetPath.getAbsolutePath());
				return;
			}
		}
		if (!targetPath.isDirectory()) {
			monitor.logError("The target path is no directory!");
			return;
		}
		
		IProcess process = context.getProcess();
		if (process instanceof IETLProcess) {
			IETLProcess etlp = (IETLProcess)process;
			for (ISource source : etlp.getSources()) {
				if (source instanceof FileSource) {
					FileSource fs = (FileSource)source;
					File file = fs.getFile();
					if (file != null) {
						File destFile = new File(targetPath, file.getName());
						if (destFile.exists() && !deleteTargetIfExists) {
							monitor.logWarn("Cannot move file " + file.getName() + " as it already exists in the target location.");
						} else {
							if (destFile.exists()) {
								if (!destFile.delete()) {
									monitor.logError("Error deleting target file - file cannot be moved!");
									continue;
								}
							}
							if (!file.renameTo(destFile)) {
								monitor.logWarn("File was not moved to " + destFile.getAbsolutePath());
							} else {
								monitor.logInfo("File " + file.getName() + " moved to " + targetPath.getAbsolutePath());
							}
						}
					}
				} else {
					monitor.logWarn("Cannot move source " + source.getName() + " as it is no FileSource.");
				}
			}
		}

	}

	/**
	 * @return the deleteTargetIfExists
	 */
	public boolean isDeleteTargetIfExists() {
		return deleteTargetIfExists;
	}

	/**
	 * @param deleteTargetIfExists the deleteTargetIfExists to set
	 */
	public void setDeleteTargetIfExists(boolean deleteTargetIfExists) {
		this.deleteTargetIfExists = deleteTargetIfExists;
	}

}
