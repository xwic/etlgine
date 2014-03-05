package de.xwic.etlgine.publish;

public class CubePublishDestination {
	private String key;
	private boolean enabled;
	private String path;
	private String urlCacheStat;
	private String urlRefreshApp;
	
	public boolean isEnabled() {
		return enabled;
	}
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
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
	
	
}
