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
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.groovy.ast.stmt.IfStatement;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IMonitor.EventType;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.publish.CubePublishDestination;
import de.xwic.etlgine.publish.CubePublisherManager;
import de.xwic.etlgine.util.FileUtils;

public class PublishDataPoolsFinalizer implements IProcessFinalizer {
	/**
	 * 
	 */
	public PublishDataPoolsFinalizer() {
		super();
	}
	
	@Override
	public void onFinish(IProcessContext context) throws ETLException {
		if (context.getResult() == Result.SUCCESSFULL) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HHmmss");
			String dpVersionName = df.format(new Date());
			
			List<CubePublishDestination> cubePublishDestinations  = CubePublisherManager.getPublishTargets();
			for (CubePublishDestination cubePublishDestination : cubePublishDestinations) {
				if (cubePublishDestination.isEnabled()) {
					context.getMonitor().logDebug("Start Datapool publish to Destination ["+cubePublishDestination.getFullKey()+"]! ");
					//create destination folder
					File target = new File(cubePublishDestination.getPath(), dpVersionName + ".datapool");
					int i = 1;
					while (target.exists()) {
						target = new File(cubePublishDestination.getPath(), dpVersionName + "_" + (i++) + ".datapool");
					}
					target.mkdirs();
					
					String datapoolSavePath = context.getProperty(cubePublishDestination.getDatapoolKey()+".datapool.path", null);
					String datapoolKey = context.getProperty(cubePublishDestination.getDatapoolKey()+".datapool.key", null);
					
					if(datapoolSavePath!= null && datapoolKey!= null) {
						String parent = context.getProperty(IContext.PROPERTY_ROOTPATH, ".");
						String dataPoolSourceFile = datapoolSavePath +"/"+ datapoolKey+".datapool";
						File datapoolForKey = new File(parent, dataPoolSourceFile);
						if (datapoolForKey.exists()) {
							try {
								copyDatapoolToDestination(context, datapoolForKey, target);
								
								refreshApplicationCubes(context, cubePublishDestination.getUrlRefreshApp());
								
								if(cubePublishDestination.getKeepVersions() >0 ) {
									cleanupPublishTarget(context, target, cubePublishDestination.getKeepVersions());
								}
								context.getMonitor().onEvent(context, EventType.DATAPOOL_POST_PUBLISH, cubePublishDestination.getFullKey());
							} catch (ETLException e) {
								context.getMonitor().logError("Publish Failed! Source["+datapoolForKey.getAbsolutePath()+"] Destination["+target.getAbsolutePath()+"]", e);
							}
						} else {
							context.getMonitor().logWarn("Datapool could not be located! " + datapoolForKey.getAbsolutePath());
						}
					} else {
						context.getMonitor().logWarn("Datapool could not be located! Path or Key is null!");
					}
				} else {
					context.getMonitor().logDebug("Skip Datapool publish to Destination ["+cubePublishDestination.getFullKey()+"]! ");
				}
			}
		}
	}

	private void cleanupPublishTarget(IProcessContext context, File target,
			int keepVersions) {
		try {
			File[] publishFolders = target.getParentFile().listFiles(new FileFilter() {
				@Override
				public boolean accept(File file) {
					return file.getName().endsWith(".datapool");
				}
			});
			
			if(!ArrayUtils.isEmpty(publishFolders)) {
		        Arrays.sort(publishFolders);
		        //System.out.println(Arrays.deepToString(publishFolders));
		        if(publishFolders.length > keepVersions) {
		        	int removeTargets = publishFolders.length - keepVersions;
			        for (File file : publishFolders) {
			        	if(removeTargets != 0) {
			        		removeNotEmpty(file);
				        	removeTargets--;
				        	context.getMonitor().logDebug("Removed ["+file.getAbsolutePath()+"]! ");
			        	}
					}
		        }
			}
		} catch(IOException e) {
			context.getMonitor().logError("Folder cleanup failed!", e);
		}
	}
	
	private static void removeNotEmpty(File file)
	    	throws IOException{
	    	if(file.isDirectory()){
	    		//directory is empty, then delete it
	    		if(file.list().length==0){
	    		   file.delete();
	    		}else{
	    		   //list all the directory contents
	        	   String files[] = file.list();
	        	   for (String temp : files) {
	        	      //construct the file structure
	        	      File fileDelete = new File(file, temp);
	        	      //recursive delete
	        	      removeNotEmpty(fileDelete);
	        	   }
	        	   //check the directory again, if empty then delete it
	        	   if(file.list().length==0){
	           	     file.delete();
	        	   }
	    		}
	    	}else{
	    		//if file, then delete it
	    		file.delete();
	    	}
	    }
	
	private void copyDatapoolToDestination(IProcessContext context, File sourceFile, File target)
			throws ETLException {
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

			context.getMonitor().logDebug("Datapool published! Source["+sourceFile.getAbsolutePath()+"] Destination["+target.getAbsolutePath()+"]");

		} catch (IOException e) {
			throw new ETLException("Error copying data pool", e);
		}
	}
	private void refreshApplicationCubes(IProcessContext context, String refreshUrl) throws ETLException {
		// now notify the foreign system
		if (refreshUrl != null && !"".equals(refreshUrl)) {
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
		} else {
			context.getMonitor().logDebug("Refresh URL is empty.");
		}
		
	}

}
