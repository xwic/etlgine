/**
 * 
 */
package de.xwic.etlgine.finalizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.FileChannel;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;

/**
 * Publishes a DataPool that is stored in the file system. The finalizer simply copies the
 * datapool to a specified directory and starts the reload process by invoking the URL
 * of the other system.
 * @author lippisch
 */
public class PublishCubeFinalizer implements IProcessFinalizer {

	private File sourceFile;
	private File targetPath;
	private String refreshUrl;

	/**
	 * 
	 */
	public PublishCubeFinalizer() {
		super();
	}

	/**
	 * @param sourceFile
	 * @param targetPath
	 * @param refreshUrl
	 */
	public PublishCubeFinalizer(File sourceFile, File targetPath, String refreshUrl) {
		super();
		this.sourceFile = sourceFile;
		this.targetPath = targetPath;
		this.refreshUrl = refreshUrl;
	}

	/**
	 * @param sourceFile
	 * @param targetPath
	 * @param refreshUrl
	 */
	public PublishCubeFinalizer(String sourceFile, String targetPath, String refreshUrl) {
		super();
		this.sourceFile = new File(sourceFile);
		this.targetPath = new File(targetPath);
		this.refreshUrl = refreshUrl;
	}

	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException {

		if (context.getResult() == Result.SUCCESSFULL) {
			
			File targetFile = new File(targetPath, sourceFile.getName());
			if (targetFile.exists()) {
				File backupFile = new File(targetPath, sourceFile.getName() + ".bak");
				if (backupFile.exists()) {
					backupFile.delete();
				}
				if (!targetFile.renameTo(backupFile)) {
					context.getMonitor().logError("Making a backup of the target file failed!");
					return;
				}
			}
			
			try {
				FileChannel inChannel = new FileInputStream(sourceFile).getChannel();
				FileChannel outChannel = new FileOutputStream(targetFile).getChannel();
		        try {
		        	int maxCount = (64 * 1024 * 1024) - (32 * 1024); // copy in blocks of 64 MB because of windows limitations
		            long size = inChannel.size();
		            long position = 0;
		            while (position < size) {
		               position += 
		                 inChannel.transferTo(position, maxCount, outChannel);
		            }	
		        } finally {
		            if (inChannel != null) inChannel.close();
		            if (outChannel != null) outChannel.close();
		        }
			} catch (Exception e) {
				throw new ETLException("Error copying file! " + e, e);
			}

			// now notify the foreign system
			if (refreshUrl != null) {
				try {
					URL url = new URL(refreshUrl);
					URLConnection connection = url.openConnection();
					if (connection instanceof HttpURLConnection) {
						HttpURLConnection httpCon = (HttpURLConnection)connection;
						httpCon.setRequestMethod("GET");
						int response = httpCon.getResponseCode();
						if (response != HttpURLConnection.HTTP_OK) {
							context.getMonitor().logError("Error on refresh: Server response is " + response);
						} else {
							context.getMonitor().logInfo("Refresh response from server successfull.");
						}
						httpCon.disconnect();
					}
					
				} catch (Exception e) {
					throw new ETLException("Error invoking refresh URL: " + e, e);
				}
			} 
			
		}

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
	 * @return the refreshUrl
	 */
	public String getRefreshUrl() {
		return refreshUrl;
	}

	/**
	 * @param refreshUrl the refreshUrl to set
	 */
	public void setRefreshUrl(String refreshUrl) {
		this.refreshUrl = refreshUrl;
	}

}
