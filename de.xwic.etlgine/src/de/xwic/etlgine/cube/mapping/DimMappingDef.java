/**
 * 
 */
package de.xwic.etlgine.cube.mapping;


/**
 * @author lippisch
 */
public class DimMappingDef {

	public enum Action { CREATE, SKIP, ASSIGN, FAIL };
	
	private String key = null;
	private String description = null; 
	private String dimensionKey = null;
	private String unmappedPath = null;
	private Action onUnmapped = Action.CREATE;
	private boolean autoCreateMapping = false;
	
	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the dimensionKey
	 */
	public String getDimensionKey() {
		return dimensionKey;
	}
	/**
	 * @param dimensionKey the dimensionKey to set
	 */
	public void setDimensionKey(String dimensionKey) {
		this.dimensionKey = dimensionKey;
	}
	/**
	 * @return the unmappedPath
	 */
	public String getUnmappedPath() {
		return unmappedPath;
	}
	/**
	 * @param unmappedPath the unmappedPath to set
	 */
	public void setUnmappedPath(String unmappedPath) {
		this.unmappedPath = unmappedPath;
	}
	/**
	 * @return the onUnmapped
	 */
	public Action getOnUnmapped() {
		return onUnmapped;
	}
	/**
	 * @param onUnmapped the onUnmapped to set
	 */
	public void setOnUnmapped(Action onUnmapped) {
		this.onUnmapped = onUnmapped;
	}
	/**
	 * @return the autoCreateMapping
	 */
	public boolean isAutoCreateMapping() {
		return autoCreateMapping;
	}
	/**
	 * @param autoCreateMapping the autoCreateMapping to set
	 */
	public void setAutoCreateMapping(boolean autoCreateMapping) {
		this.autoCreateMapping = autoCreateMapping;
	}
	
}
