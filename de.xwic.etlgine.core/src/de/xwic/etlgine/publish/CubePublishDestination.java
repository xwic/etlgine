package de.xwic.etlgine.publish;

import java.io.File;

public class CubePublishDestination {
	private String datapoolKey;
	
	private String key;
	private boolean enabled;
	private File path;
	private String urlCacheStat;
	private String urlRefreshApp;
	
	private int keepVersions;
	
	public String getFullKey() {
		return datapoolKey +"-"+getKey();
	}
	
	public String getDatapoolKey() {
		return datapoolKey;
	}
	public void setDatapoolKey(String datapoolKey) {
		this.datapoolKey = datapoolKey;
	}
	public boolean isEnabled() {
		return enabled;
	}
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	public File getPath() {
		return path;
	}
	public void setPath(File path) {
		this.path = path;
	}
	public String getUrlCacheStat() {
		return urlCacheStat;
	}
	public void setUrlCacheStat(String urlCacheStat) {
		this.urlCacheStat = urlCacheStat;
	}
	public String getUrlRefreshApp() {
		return urlRefreshApp;
	}
	public void setUrlRefreshApp(String urlRefreshApp) {
		this.urlRefreshApp = urlRefreshApp;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getKeepVersions() {
		return keepVersions;
	}
	public void setKeepVersions(int keepVersions) {
		this.keepVersions = keepVersions;
	}
	
	
}
