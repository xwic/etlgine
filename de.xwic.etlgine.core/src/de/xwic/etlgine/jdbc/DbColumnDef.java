/**
 * 
 */
package de.xwic.etlgine.jdbc;

import java.sql.Types;

import de.xwic.etlgine.IColumn;

/**
 * @author lippisch
 *
 */
public class DbColumnDef {

	private String name = null;
	private IColumn column = null;
	private String typeName = null;
	private int type = 0;
	private int size = 0;
	private int scale = 0;
	private boolean allowsNull = false;
	private boolean readOnly = false;
	
	/**
	 * Construct just with a name.
	 * @param name
	 */
	public DbColumnDef(String name) {
		super();
		this.name = name;
	}
	
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
	public DbColumnDef(String name, int type, int size, int scale, boolean allowsNull) {
		super();
		this.name = name;
		this.type = type;
		this.size = size;
		this.scale = scale;
		this.allowsNull = allowsNull;
	}

	/**
	 * @param name
	 * @param type
	 * @param typeName
	 * @param size
	 * @param scale
	 * @param allowsNull
	 */
	public DbColumnDef(String name, int type, String typeName, int size, int scale, boolean allowsNull) {
		super();
		this.name = name;
		this.type = type;
		this.typeName = typeName;
		this.size = size;
		this.scale = scale;
		this.allowsNull = allowsNull;
		// oracle NUMBER/NUMERIC convertion
		if (type == Types.NUMERIC && scale == 0) {
			// must be an integer
			if (size > 10) {
				// treat as bigint
				this.type = Types.BIGINT;
			} else {
				// treat as int
				this.type = Types.INTEGER;
			}
		}
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
	 * @return the scale
	 */
	public int getScale() {
		return scale;
	}

	/**
	 * @param scale the scale to set
	 */
	public void setScale(int precision) {
		this.scale = precision;
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

	/**
	 * @return the column
	 */
	public IColumn getColumn() {
		return column;
	}

	/**
	 * @param column the column to set
	 */
	public void setColumn(IColumn column) {
		this.column = column;
	}

	/**
	 * @return the typeName
	 */
	public String getTypeName() {
		return typeName;
	}

	/**
	 * @param typeName the typeName to set
	 */
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	/**
	 * @return the readOnly
	 */
	public boolean isReadOnly() {
		return readOnly;
	}

	/**
	 * @param readOnly the readOnly to set
	 */
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	/**
	 * @return the full SQL type name with length for varchar
	 */
	public String getTypeNameDetails() {
		String s = typeName;
		switch (type) {
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			s += "(" + size + ")";
			break;
		case Types.NUMERIC:
			s += "(" + size + "," + scale + ")";
			break;
		}
		return s;
	}
	
	@Override
	public String toString() {
		return "\"" + name + "\" " + getTypeNameDetails();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (allowsNull ? 1231 : 1237);
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (readOnly ? 1231 : 1237);
		result = prime * result + scale;
		result = prime * result + size;
		result = prime * result + type;
		result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DbColumnDef other = (DbColumnDef) obj;
		if (allowsNull != other.allowsNull)
			return false;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (readOnly != other.readOnly)
			return false;
		if (scale != other.scale)
			return false;
		if (size != other.size)
			return false;
		if (type != other.type)
			return false;
		if (typeName == null) {
			if (other.typeName != null)
				return false;
		} else if (!typeName.equals(other.typeName))
			return false;
		return true;
	}

	
}
