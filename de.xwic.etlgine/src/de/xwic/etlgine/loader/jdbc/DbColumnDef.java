/**
 * 
 */
package de.xwic.etlgine.loader.jdbc;

/**
 * @author lippisch
 *
 */
public class DbColumnDef {

	private String name = null;
	private int type = 0;
	private int size = 0;
	private boolean allowsNull = false;
	
	/**
	 * @param name
	 * @param type
	 */
	public DbColumnDef(String name, int type) {
		super();
		this.name = name;
		this.type = type;
	}
	
	/**
	 * @param name
	 * @param type
	 * @param size
	 * @param allowsNull
	 */
	public DbColumnDef(String name, int type, int size, boolean allowsNull) {
		super();
		this.name = name;
		this.type = type;
		this.size = size;
		this.allowsNull = allowsNull;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the type
	 */
	public int getType() {
		return type;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(int type) {
		this.type = type;
	}
	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}
	/**
	 * @param size the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}
	/**
	 * @return the allowsNull
	 */
	public boolean isAllowsNull() {
		return allowsNull;
	}
	/**
	 * @param allowsNull the allowsNull to set
	 */
	public void setAllowsNull(boolean allowsNull) {
		this.allowsNull = allowsNull;
	}
	
	
}
