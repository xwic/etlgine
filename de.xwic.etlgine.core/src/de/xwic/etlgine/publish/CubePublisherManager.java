package de.xwic.etlgine.publish;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.IContext;
import de.xwic.etlgine.server.ETLgineServer;

public class CubePublisherManager {
	private static final Log log = LogFactory.getLog(CubePublisherManager.class);
	
	private static CubePublisherManager instance = null;
	
	private static final String PUBLISH_DESTINATIONS_KEY = ".datapool.publish";
	private static final String PUBLISH_ENABLED_SUFFIX = ".publish.enabled";
	private static final String PUBLISH_PATH_SUFFIX = ".publish.path";
	private static final String PUBLISH_URL_CACHE_SUFFIX = ".publish.url.cachestat";
	private static final String PUBLISH_URL_REFRESH_SUFFIX = ".publish.url.refreshapp";
	private static final String PUBLISH_KEEP_VERSIONS_SUFFIX = ".publish.keep.versions";
	
	private static List<CubePublishDestination> publishTargets = null;
	
	/**
	 * 
	 */
	private CubePublisherManager() {
		instance = this;
	}
	
	/**
	 * Returns the instance.
	 * @return
	 */
	public static CubePublisherManager getInstance() {
		if (instance == null) {
			instance = new CubePublisherManager();
		}
		return instance;
	}	
	
	public void fillPublishTargets(IContext context, String datapoolKey) {
		List<CubePublishDestination> result = new ArrayList<CubePublishDestination>();
		
		String publishDestinations = context.getProperty(datapoolKey + PUBLISH_DESTINATIONS_KEY, null);
		if(publishDestinations != null)  {
			StringTokenizer stk = new StringTokenizer(publishDestinations, ",; ");
			while (stk.hasMoreTokens()) {
				String destinationKey = stk.nextToken();
				CubePublishDestination destination = validateDestination(destinationKey, context, datapoolKey);
				if(destination != null) {
					result.add(destination);					
				}
			}
		} else {
			log.warn("No publish destinations set!");
		}
		
		setPublishTargets(result);
	}
	
	private static CubePublishDestination validateDestination(String destinationKey, IContext context, String datapoolKey) {
		CubePublishDestination destination = null;
		
		boolean publishEnabled = context.getPropertyBoolean(destinationKey + PUBLISH_ENABLED_SUFFIX, false);
		String publishPath = context.getProperty(destinationKey + PUBLISH_PATH_SUFFIX, null);
		String publishUrlCacheStat = context.getProperty(destinationKey + PUBLISH_URL_CACHE_SUFFIX, null);
		String publishUrlRefreshApp = context.getProperty(destinationKey + PUBLISH_URL_REFRESH_SUFFIX, null);
		int publishKeepVersions = context.getPropertyInt(destinationKey + PUBLISH_KEEP_VERSIONS_SUFFIX, 10);
		
		if (!StringUtils.isEmpty(publishPath)) {
			String parent = null;
			if(publishPath.startsWith("{ROOT_PATH}")) {
				parent = context.getProperty(IContext.PROPERTY_ROOTPATH, ".");
				publishPath = publishPath.replace("{ROOT_PATH}", "");
			}
			File destinationFile = new File(parent,publishPath);
			if(destinationFile.isDirectory()) {
				
				destination = new CubePublishDestination();
				destination.setDatapoolKey(datapoolKey);
				destination.setKey(destinationKey);
				destination.setEnabled(publishEnabled);
				destination.setPath(destinationFile);
				destination.setUrlCacheStat(publishUrlCacheStat);
				destination.setUrlRefreshApp(publishUrlRefreshApp);
				destination.setKeepVersions(publishKeepVersions);
				
				log.info("Valid destination settings for key [" + destinationKey + "]");
			} else {
				log.warn("Path does not exist [" + destinationFile.getAbsolutePath() + "]");
			}
		}
		
		return destination;
	}

	public static List<CubePublishDestination> getPublishTargets() {
		return publishTargets;
	}

	public static void setPublishTargets(List<CubePublishDestination> pPublishTargets) {
		publishTargets = pPublishTargets;
	}
	
	public static void setTargetEnabled(String publishTargetKey, boolean enabled) {
		for (int i = 0; i < publishTargets.size(); i++) {
			CubePublishDestination publishTarget = publishTargets.get(i);
			if(publishTarget.getFullKey().equals(publishTargetKey)) {
				publishTargets.get(i).setEnabled(enabled);
			}
		}
	}
	
}
