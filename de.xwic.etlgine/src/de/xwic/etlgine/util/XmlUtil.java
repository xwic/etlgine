/**
 * 
 */
package de.xwic.etlgine.util;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Properties;

import org.dom4j.DocumentException;
import org.dom4j.Element;

/**
 * Common functionalities for xml (dom4j) processing.
 * @author lippisch
 */
public class XmlUtil {

	/**
	 * Load all child elements as properties to the bean.
	 * @param con
	 * @param conDetail
	 * @throws DocumentException 
	 */
	@SuppressWarnings("unchecked")
	public static void elementToBean(Element con, Object bean, boolean ignoreMissing) throws DocumentException {
		
		Class<?> beanClass = bean.getClass();
		
		for (Iterator<Element> it = con.elementIterator(); it.hasNext(); ) {
			Element elm = it.next();
			try {
				PropertyDescriptor pd = new PropertyDescriptor(elm.getName(), beanClass);
				Method writeMethod = pd.getWriteMethod();
				
				if (pd.getPropertyType().equals(Properties.class)) {
					Properties prop = new Properties();
					for (Iterator<Element> itProp = elm.elementIterator("property"); itProp.hasNext(); ) {
						Element eProp = itProp.next();
						prop.put(eProp.attributeValue("name"), eProp.getTextTrim());
					}
					writeMethod.invoke(bean, prop);
				} else {
					String s = elm.getTextTrim();
					if (Integer.class.equals(pd.getPropertyType()) || int.class.equals(pd.getPropertyType())) {
						writeMethod.invoke(bean, new Integer(s));
					}else if (Long.class.equals(pd.getPropertyType()) || long.class.equals(pd.getPropertyType())) {
						writeMethod.invoke(bean, new Long(s));
					} else {
						writeMethod.invoke(bean, s);
					}
				}
			} catch (Exception e) {
				if (!ignoreMissing) {
					throw new DocumentException("Error reading attribute '" + elm.getName() + "'", e);
				}
			}
		}
		
	}

	
}
