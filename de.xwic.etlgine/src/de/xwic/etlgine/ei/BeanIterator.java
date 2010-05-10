/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author lippisch
 */
public class BeanIterator<E> implements ResourceIterator<E> {

	private Class<E> clazz;
	private ResultSet rs;
	private boolean hasNext;
	
	private class PropMap {
		String colName;
		int sqlType;
		Class<?> propType;
		Method mWrite;
	}

	private PropMap[] colMapping;
	
	/**
	 * @param clazz
	 * @param rs
	 * @throws IntrospectionException 
	 * @throws EIException 
	 */
	public BeanIterator(Class<E> clazz, ResultSet rs) throws SQLException, IntrospectionException, EIException {
		this(clazz, rs, false);
	}
	/**
	 * @param clazz
	 * @param rs
	 * @throws IntrospectionException 
	 * @throws EIException 
	 */
	@SuppressWarnings("unchecked")
	public BeanIterator(Class<E> clazz, ResultSet rs, boolean ignoreMissingProperties) throws SQLException, IntrospectionException, EIException {
		super();
		this.clazz = clazz;
		this.rs = rs;

		BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		
		// evaluate columns
		ResultSetMetaData metaData = rs.getMetaData();

		colMapping = new BeanIterator.PropMap[metaData.getColumnCount()];
		
		for (int i = 0; i < metaData.getColumnCount(); i++) {
			PropMap propMap = null;
			String colName = metaData.getColumnName(i + 1);
			// try to find exact match
			PropertyDescriptor match = null;
			for (PropertyDescriptor pd : propertyDescriptors) {
				if (pd.getName().equals(colName)) {
					match = pd;
					break;
				}
			}
			if (match == null) {
				// try to find one ignoring upper/lower case
				for (PropertyDescriptor pd : propertyDescriptors) {
					if (pd.getName().equalsIgnoreCase(colName)) {
						match = pd;
						break;
					}
				}
			}
			
			if (match == null) { // still no match? convert sql to bean pattern.
				String s = generatePropertyName(colName);
				for (PropertyDescriptor pd : propertyDescriptors) {
					if (pd.getName().equalsIgnoreCase(s)) {
						match = pd;
						break;
					}
				}
				
			}
			
			if (match != null) {
				propMap = new PropMap();
				propMap.colName = colName;
				propMap.mWrite = match.getWriteMethod();
				propMap.propType = match.getPropertyType();
				propMap.sqlType = metaData.getColumnType(i + 1);
			} else {
				if (!ignoreMissingProperties) {
					throw new EIException("The column '" + colName + "' can not be matched to any properties of bean " + clazz.getName());
				}
			}
			
			colMapping[i] = propMap;
		}
		
		hasNext = rs.next();
	}

	/**
	 * Converts a sql name string like CUSTOMER_NAME into property type (customerName);
	 * @param colName
	 * @return
	 */
	private String generatePropertyName(String colName) {
		StringBuilder sb = new StringBuilder();
		boolean upperNext = false;
		for (int i = 0; i < colName.length(); i++) {
			char c = colName.charAt(i);
			
			if (c >= 'A' && c <= 'Z') {
				if (upperNext) {
					sb.append(c);
					upperNext = false;
				} else {
					sb.append(Character.toLowerCase(c));
				}
				
			} else if (c == '_') {
				upperNext = true;
			} else if (c >= 'a' && c <= 'z') {
				if (upperNext) {
					sb.append(Character.toUpperCase(c));
					upperNext = false;
				} else {
					sb.append(c);
				}
			} else {
				sb.append(c);
				upperNext = false;
			}
			
		}
		return sb.toString();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return hasNext;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public E next() {
		
		E bean;
		try {
			bean = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initializing bean type " + clazz.getName());
		}
		
		try {
			for (int i = 0; i < colMapping.length; i++) {
				PropMap pm = colMapping[i];
				int colIdx = i + 1;
				if (pm != null) {
					
					Object value = null;
					// ----------- STRING
					if (pm.propType.equals(String.class)) {		
						value = rs.getString(colIdx);
						
					// ----------- Integer | int
					} else if (pm.propType.equals(Integer.class) || pm.propType.equals(int.class)) {
						switch (pm.sqlType) {
						case Types.VARCHAR:
						case Types.CHAR:
						case Types.CLOB:
							String s = rs.getString(colIdx);
							value = new Integer(s); // convert
							break;
						default:
							value = rs.getInt(colIdx);
							if (rs.wasNull()) {
								value = null;
							}
						}
						// ----------- Long | long
					} else if (pm.propType.equals(Long.class) || pm.propType.equals(long.class)) {
						switch (pm.sqlType) {
						case Types.VARCHAR:
						case Types.CHAR:
						case Types.CLOB:
							String s = rs.getString(colIdx);
							value = new Long(s); // convert
							break;
						default:
							value = rs.getLong(colIdx);
							if (rs.wasNull()) {
								value = null;
							}
						}
					// ----------- Double
					} else if (pm.propType.equals(Double.class) || pm.propType.equals(double.class)) {
						switch (pm.sqlType) {
						case Types.VARCHAR:
						case Types.CHAR:
						case Types.CLOB:
							String s = rs.getString(colIdx);
							value = new Double(s); // convert
							break;
						default:
							value = rs.getDouble(colIdx);
							if (rs.wasNull()) {
								value = null;
							}
						}
					// ----------- Date
					} else if (pm.propType.equals(Date.class)) {
						switch (pm.sqlType) {
						case Types.VARCHAR:
						case Types.CHAR:
						case Types.CLOB:
							String s = rs.getString(colIdx);
							if (s != null) {
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd", Locale.US);
								value = sdf.parse(s);
							}
							break;
						default:
							value = rs.getDate(colIdx);
						}
					}
					pm.mWrite.invoke(bean, value);
				}
			}
			hasNext = rs.next();
		} catch (Exception e) {
			throw new RuntimeException("Error reading data from resultset: " + e, e);
		}
		return bean;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported.");
	}
	/* (non-Javadoc)
	 * @see com.netapp.pulse.ei.ResourceIterator#close()
	 */
	@Override
	public void close() {
		try {
			rs.getStatement().close();
			rs.close();
		} catch (SQLException e) {
			throw new RuntimeException("Error closing ResultSet: " + e, e);
		}
	}

}
