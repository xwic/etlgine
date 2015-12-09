/**
 * 
 */
package de.xwic.etlgine.finalizer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.IJobFinalizer;
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
public class MoveFileFinalizer implements IProcessFinalizer, IJobFinalizer {

	private File sourceFile = null;
	private File targetPath;
	
	private boolean deleteTargetIfExists = true;
	private IMonitor monitor;
	private boolean moveOnError = false;
	
	private List<File> moveFiles = null;
	private String prefix = null;
	
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

	/**
	 * Registers this finalizer in the job (for final execution)
	 * @param job
	 */
	public void register(IJob job) {
		setMonitor(job.getProcessChain().getMonitor());
		job.getProcessChain().addJobFinalizer(this);
		moveFiles = new ArrayList<File>();
	}
	

	public void onFinish(IJob job) throws ETLException {
		
		if (moveFiles != null && moveFiles.size() > 0) {
			if (!moveOnError && job.getState() != State.FINISHED) {
				monitor.logWarn("File(s) are not moved because jobs exited with errors");
				return;
			}
			
			// move files
			List<File> files = new ArrayList<File>(moveFiles);
			moveFiles = null;
			for (File file : files) {
				moveFile(file);
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException{
		
		setMonitor(context.getMonitor());
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
		
		if (!moveOnError && (context.getResult() == Result.FAILED || context.getResult() == Result.FINISHED_WITH_ERRORS)) {
			monitor.logWarn("File(s) are not moved because process exited with errors");
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
						if (file != null) {
							if (file.exists()) {
								if (!moveFile(file))  {
									context.setResult(Result.FINISHED_WITH_ERRORS);
								}
							} else {
								monitor.logWarn("Cannot move source " + source.getName() + " as as the file does not exist: " + file.getAbsolutePath());
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

		if (moveFiles != null) {
			// move file later in job finished event
			moveFiles.add(file);
			return true;
		}
		
		if (!file.exists()) {
			monitor.logWarn("Cannot move file " + file.getName() + " because it does not exist.");
			return false;
		}
		
		File destFile = new File(targetPath, prefix != null ? prefix + file.getName() : file.getName());
		if (destFile.equals(file)) {
			monitor.logWarn("Cannot move file " + file.getName() + " into source location");
			return false;
		}
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

	/**
	 * @return the moveOnError
	 */
	public boolean isMoveOnError() {
		return moveOnError;
	}

	/**
	 * Set to true, if the file(s) should get moved even if the process exited with an error.
	 * Default is false.
	 * @param moveOnError the moveOnError to set
	 */
	public void setMoveOnError(boolean moveOnError) {
		this.moveOnError = moveOnError;
	}

	/**
	 * @return the sourceFile
	 */
	public File getSourceFile() {
		return sourceFile;
	}

	/**
	 * @param sourceFile the sourceFile to set
	 */
	public void setSourceFile(File sourceFile) {
		this.sourceFile = sourceFile;
	}

	/**
	 * @return the targetPath
	 */
	public File getTargetPath() {
		return targetPath;
	}

	/**
	 * @param targetPath the targetPath to set
	 */
	public void setTargetPath(File targetPath) {
		this.targetPath = targetPath;
	}

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor() {
		return monitor;
	}

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor) {
		this.monitor = monitor;
	}

	/**
	 * @return the prefix
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * @param prefix the prefix to set
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

}
