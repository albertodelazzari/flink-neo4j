package org.apache.flink.streaming.connectors.neo4j;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 * 
 * @author Alberto De Lazzari
 *
 */
public class Neo4JDriverWrapperTest extends Neo4JBaseEmbeddedTest {

	@Test
	public void testConnection() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("neo4j.url", "bolt://localhost:7687");
		parameters.put("neo4j.auth.username", "neo4j");
		parameters.put("neo4j.auth.password", "password");
		
		Neo4JDriverWrapper driverWrapper = new Neo4JDriverWrapper(parameters);
		assertNotNull(driverWrapper);
		Session session = driverWrapper.session();
		assertNotNull(session);

		StatementResult result = session.run("MATCH (n) return n");
		assertNotNull(result);
		assertTrue(result.hasNext()); 
		session.close();
		assertTrue(!session.isOpen());
	}
}