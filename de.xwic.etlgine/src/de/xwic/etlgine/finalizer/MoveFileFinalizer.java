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
import de.xwic.etlgine.Result;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 *
 */
public class MoveFileFinalizer implements IProcessFinalizer {

	private File sourceFile = null;
	private File targetPath;
	
	private boolean deleteTargetIfExists = true;
	private IMonitor monitor;
	
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

	/**
	 * @param targetPath
	 */
	public MoveFileFinalizer(File targetPath, File sourceFile) {
		super();
		this.targetPath = targetPath;
		this.sourceFile = sourceFile;
	}

	/**
	 * @param targetPath
	 */
	public MoveFileFinalizer(String targetPath, String sourceFileName) {
		super();
		this.targetPath = new File(targetPath);
		this.sourceFile = new File(sourceFileName);
	}


	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) {
		
		monitor = context.getMonitor();
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
	
		if (sourceFile != null) {
			if (!moveFile(sourceFile)) {
				context.setResult(Result.FINISHED_WITH_ERRORS);
			}
		} else {
			IProcess process = context.getProcess();
			if (process instanceof IETLProcess) {
				IETLProcess etlp = (IETLProcess)process;
				for (ISource source : etlp.getSources()) {
					if (source instanceof FileSource) {
						FileSource fs = (FileSource)source;
						File file = fs.getFile();
						if (file != null && file.exists()) {
							if (!moveFile(file))  {
								context.setResult(Result.FINISHED_WITH_ERRORS);
							}
						}
					} else {
						monitor.logWarn("Cannot move source " + source.getName() + " as it is no FileSource.");
					}
				}
			}
		}

	}

	/**
	 * @param file
	 * @return 
	 */
	private boolean moveFile(File file) {

		if (!file.exists()) {
			monitor.logWarn("Can not move file " + file.getName() + " because it does not exist.");
			return false;
		}
		
		File destFile = new File(targetPath, file.getName());
		if (destFile.exists() && !deleteTargetIfExists) {
			monitor.logWarn("Cannot move file " + file.getName() + " as it already exists in the target location.");
			return false;
		} else {
			if (destFile.exists()) {
				if (!destFile.delete()) {
					monitor.logError("Error deleting target file - file cannot be moved!");
					return false;
				}
			}
			if (!file.renameTo(destFile)) {
				monitor.logWarn("File was not moved to " + destFile.getAbsolutePath());
				return false;
			} else {
				monitor.logInfo("File " + file.getName() + " moved to " + targetPath.getAbsolutePath());
				return true;
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
