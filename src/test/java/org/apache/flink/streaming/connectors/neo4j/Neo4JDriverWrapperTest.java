package org.apache.flink.streaming.connectors.neo4j;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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