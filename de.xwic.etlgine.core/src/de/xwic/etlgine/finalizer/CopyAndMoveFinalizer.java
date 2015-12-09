package de.xwic.etlgine.finalizer;

import java.io.File;
import java.io.IOException;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.sources.FileSource;
import de.xwic.etlgine.util.FileUtils;

/**
 * The class(finalizer) copies and moves a file according to the copyFlag value:
 * if the flag is on:
 * - if the move destination(destToMove) is empty or null it just copies the file to destToCopy
 * - if the move destination(destToMove) is not empty or not null it copies the file to destToCopy and then moves it to destToMove
 * if the flag is off it only moves the file to destToMove
 * 
 * limitations: 
 * it only works for one source
 * it only works when the method onFinish from MoveFileFinalizer throws an ETLException
 * 
 * @author RobertD
 *
 */
public class CopyAndMoveFinalizer extends MoveFileFinalizer {
	
	/** The optional source path of the file to be copied and/or moved. If it is not specified 
	 * it is taken from the process source or from the config file if there is specified a cofiguration key 
	 */
	protected String sourcePath;
	
	/**
	 * The name of the file to be copied and/or moved
	 */
	protected String fileName;
	
	/**
	 * The configuration key from server.properties that contains all destination and source paths
	 */
	protected String serverKey;
	
	/**
	 * The destination path where the file shall be copied 
	 */
	protected String destToCopy;
	
	/**
	 * The destination path where the file shall be moved
	 */
	protected String destToMove;
	
	/**
	 * The flag used to configure if the file shall be copied in case of error
	 */
	protected boolean copyOnError;  
	
	/**
	 * The flag used to specify if the file shall be copied
	 */
	protected boolean copyFlag;
	
	/**
	 * 
	 * @param destToCopy The destination path where the file shall be copied 
	 * @param destToMove The destination path where the file shall be moved
	 * @param copyFlag The flag used to specify if the file shall be copied
	 * @throws ETLException 
	 */
	public CopyAndMoveFinalizer(String destToCopy, String destToMove, boolean copyFlag) throws ETLException {
		
		super(destToMove); 
		if (destToCopy == null || "".equals(destToCopy)) {
			throw new ETLException("The copy destination should not be empty or null");
		}
		 
		this.destToCopy = destToCopy;
		this.destToMove = destToMove;
		this.copyFlag = copyFlag;
	}

	/**
	 * 
	 * @param sourcePath
	 * @param destToCopy The destination path where the file shall be copied 
	 * @param destToMove The destination path where the file shall be moved
	 * @param copyFlag The flag used to specify if the file shall be copied
	 * @throws ETLException
	 */
	public CopyAndMoveFinalizer(String sourcePath, String destToCopy, String destToMove, boolean copyFlag) throws ETLException{
		
		super(destToMove,sourcePath);
		
		if (sourcePath == null || "".equals(sourcePath)){
			throw new ETLException("SourcePath should not be empty or null");
		}
		
		if (!new File(sourcePath).exists()) {
			throw new ETLException("The sourcePath does not exist" + sourcePath);
		}
		
		if (destToCopy == null || "".equals(destToCopy)) {
			throw new ETLException("The copy destination should not be empty or null");
		}
		
		this.sourcePath = sourcePath;
		this.fileName = sourcePath.substring(sourcePath.lastIndexOf(File.separator) + 1);
		this.destToCopy = destToCopy;
		this.destToMove = destToMove;
		this.copyFlag = copyFlag;
	}
	
	/**
	 * 
	 * @param serverKey The configuration key from server.properties that contains all destination and source paths
	 * @param copyFlag The flag used to specify if the file shall be copied
	 * @throws ETLException
	 */
	public CopyAndMoveFinalizer(String serverKey, boolean copyFlag) throws ETLException {
		
		super("");
		if (serverKey == null || "".equals(serverKey)){
			throw new ETLException("ServerKey should not be empty or null");
		}
		
		this.serverKey = serverKey;
		this.copyFlag = copyFlag;
		
	}

	/*
	 * (non-Javadoc)
	 * @see com.netapp.option4.etl.finalizer.MoveFileFinalizer1#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException{
		
		
		if (serverKey != null) {
			
			sourcePath = context.getProperty(serverKey + ".source.path");
			fileName = sourcePath.substring(sourcePath.lastIndexOf(File.separator) + 1);
			
			if (!new File(sourcePath).exists()) {
				context.getMonitor().logWarn("File(s) are not copied because the sourcePath does not exist");
				throw new ETLException("The sourcePath does not exist" + sourcePath);
			}
			
			destToCopy = context.getProperty(serverKey + ".dest.path1");
			destToMove = context.getProperty(serverKey + ".dest.path2");
			
			setTargetPath(new File(destToMove));
			setSourceFile(new File(sourcePath));
			
		}
		
		if (sourcePath == null) {
			IProcess process = context.getProcess();
			if (process instanceof IETLProcess) {
				IETLProcess etlp = (IETLProcess)process;
				for (ISource source : etlp.getSources()) {
					if (source instanceof FileSource) {
						FileSource fs = (FileSource)source;
						File file = fs.getFile();
						if (file != null) {
							if (file.exists()) {
								sourcePath = file.getAbsolutePath();
								fileName = sourcePath.substring(sourcePath.lastIndexOf(File.separator) + 1);
								setSourceFile(new File(sourcePath));
							}
							else{
								context.getMonitor().logWarn("The sourcePath does not exist" + sourcePath);
								throw new ETLException("The sourcePath does not exist" + sourcePath);
							}
						}
					} else {
						context.getMonitor().logWarn(" There is no FileSource" + source.getName());
						throw new ETLException("There is no FileSource" + source.getName());
					}
				}
			}
		}
		
	
		if (!copyOnError && (context.getResult() == Result.FAILED || context.getResult() == Result.FINISHED_WITH_ERRORS)) {
			context.getMonitor().logWarn("File(s) are not copied because process exited with errors");
			return;
		}
		
		if (copyFlag) {
			
			if(destToMove == null || "".equals(destToMove)) {
				
				context.getMonitor().logInfo("Copying file " + sourcePath + " to " + destToCopy + fileName);
				
				//check if destToCopy is there and if not create it
				if (!new File(destToCopy).exists()) {
					new File(destToCopy).mkdir();
				}
				
				try {
					FileUtils.copyFile(new File(sourcePath), new File(destToCopy + fileName));
				} catch (IOException e) {
					context.getMonitor().logError(e.getMessage(), e);
					throw new ETLException("The file could not be copied: " + destToCopy + fileName );
				}
			} else {
			
				context.getMonitor().logInfo("Copying file " + sourcePath + " to " + destToCopy + fileName);
				
				//check if destToCopy is there and if not create it
				if (!new File(destToCopy).exists()) {
					new File(destToCopy).mkdir();
				}
				
				
				try {
					FileUtils.copyFile(new File(sourcePath), new File(destToCopy + fileName));
				} catch (IOException e) {
					context.getMonitor().logError(e.getMessage(), e);
						throw new ETLException("The file could not be copied: " + destToCopy + fileName );
				}
				
				super.onFinish(context);
			}
		} else {
			
			if (destToMove == null || "".equals(destToMove)) {
				context.getMonitor().logWarn("The move destination should not be empty or null");
				  throw new ETLException("The move destination should not be empty or null");
			}
			super.onFinish(context);
		}

	}
	
	/**
	 * 
	 * @param copyOnError
	 */
	public void setCopyOnError(boolean copyOnError) {
		this.copyOnError = copyOnError;
	}
}