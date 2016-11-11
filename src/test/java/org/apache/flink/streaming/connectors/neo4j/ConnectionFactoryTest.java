package org.apache.flink.streaming.connectors.neo4j;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class ConnectionFactoryTest {

	// @BeforeClass
	public static void init() {
		GraphDatabaseBuilder builder = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(new File("tmp/data/graph.db"));
		builder.newGraphDatabase();
	}

	@Test
	public void testConnection() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("neo4j.url", "bolt://localhost:7687");
		parameters.put("neo4j.auth.username", "neo4j");
		parameters.put("neo4j.auth.password", "password");

		Driver driver = ConnectionFactory.getDriver(parameters);
		assertNotNull(driver);

		Session session = driver.session();
		assertNotNull(session);

		assertTrue(session.isOpen());

		session.close();
		assertFalse(session.isOpen());
	}
}
