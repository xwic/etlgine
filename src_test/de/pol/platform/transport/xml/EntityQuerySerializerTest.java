/**
 * 
 */
package de.pol.platform.transport.xml;

import de.xwic.appkit.core.dao.EntityQuery;
import de.xwic.appkit.core.model.queries.PropertyQuery;
import de.xwic.appkit.core.transport.xml.EntityQuerySerializer;
import de.xwic.appkit.core.transport.xml.TransportException;
import junit.framework.TestCase;

/**
 * @author Florian Lippisch
 *
 */
public class EntityQuerySerializerTest extends TestCase {

	public void testSerialize() throws TransportException {
		PropertyQuery query = new PropertyQuery();
		query.addEquals("name", "Abc");
		query.addLeftOuterJoinProperty("kontakt");
		PropertyQuery sub = new PropertyQuery();
		sub.addOrEquals("ho", new Integer(1));
		query.addSubQuery(sub);
		
		String data = EntityQuerySerializer.queryToString(query);
		
		System.out.println(data);
		System.out.println("size: " + data.length());
		
		EntityQuery q2 = EntityQuerySerializer.stringToQuery(data);
		System.out.println(query);
		System.out.println(q2);
		
		assertEquals(query, q2);
		
	}
	
}
