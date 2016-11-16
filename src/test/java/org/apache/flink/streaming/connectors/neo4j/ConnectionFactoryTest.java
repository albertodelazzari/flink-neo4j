package org.apache.flink.streaming.connectors.neo4j;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

/**
 * This is not a unit test it's like more an integration one. This should be
 * rewritten
 * 
 * @author Alberto De Lazzari
 *
 */
public class ConnectionFactoryTest {

	@Test
	public void testConnection() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("neo4j.url", "bolt://localhost:7687");
		parameters.put("neo4j.auth.username", "neo4j");
		parameters.put("neo4j.auth.password", "password");

		assertNotNull(ConnectionFactory.getDriver(parameters));
		
		Driver driver = Mockito.mock(Driver.class);
		Session session = Mockito.mock(Session.class);
		Mockito.when(driver.session()).thenReturn(session);
		
		Mockito.when(session.isOpen()).thenReturn(true);
		assertTrue(session.isOpen());
		
		session.close();
		Mockito.when(session.isOpen()).thenReturn(false);
		assertFalse(session.isOpen());
	}
}