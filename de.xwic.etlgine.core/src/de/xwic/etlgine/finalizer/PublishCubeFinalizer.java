/**
 * 
 */
package de.xwic.etlgine.finalizer;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.util.FileUtils;

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

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HHmmss");
			String dpVersionName = df.format(new Date());
			
			File target = new File(targetPath, dpVersionName + ".datapool");
			int i = 1;
			while (target.exists()) {
				target = new File(targetPath, dpVersionName + "_" + (i++) + ".datapool");
			}
			target.mkdirs();
			
			// copy DataPool
			try {
				List<File> sourceFiles = new ArrayList<File>();
				sourceFiles.add(sourceFile);
				
				// add cube files
				String fName = sourceFile.getName();
				int idx = fName.indexOf('.');
				if (idx == -1) {
					idx = fName.length();
				}
				final String prefix = fName.substring(0, idx);
				File[] cubes = sourceFile.getParentFile().listFiles(new FileFilter() {
					@Override
					public boolean accept(File file) {
						return file.getName().startsWith(prefix) && file.getName().endsWith(".cube");
					}
				});
				for (File f : cubes) {	// add to list
					sourceFiles.add(f);
				}
				
				for (File f : sourceFiles) {
					context.getMonitor().logInfo("Copy file " + f.getName() + " to " + target.getName() + " (" + (f.length() / 1024) + "kByte)");
					FileUtils.copyFile(f, target);
				}
				
			} catch (IOException e) {
				throw new ETLException("Error copying data pool", e);
			}
			
			// now notify the foreign system
			if (refreshUrl != null) {
				try {
					URL url = new URL(refreshUrl);
					context.getMonitor().logInfo("Sending DataPool refresh signal to "  + url.toExternalForm());
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
