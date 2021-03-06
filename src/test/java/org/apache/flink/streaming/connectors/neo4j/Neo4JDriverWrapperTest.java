package org.apache.flink.streaming.connectors.neo4j;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 * 
 * @author Alberto De Lazzari
 *
 */
public class Neo4JDriverWrapperTest extends Neo4JBaseEmbeddedConfig {

	@Test
	public void testConfiguration() {
		Neo4JDriverWrapper driverWrapper = new Neo4JDriverWrapper(neo4JConfig);
		assertNotNull(driverWrapper);
		assertNotNull(driverWrapper.driver);
	}

	@Test
	public void testConnection() {
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