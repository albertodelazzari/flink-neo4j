package org.apache.flink.embedded.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.connectors.neo4j.Neo4JDriverWrapper;
import org.apache.flink.streaming.connectors.neo4j.Neo4JDriverWrapperMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.harness.junit.Neo4jRule;

public class Neo4JBaseEmbeddedTest {

	public static final String DEFAULT_URL = "bolt://localhost:7687";

	public static final String DEFAULT_USERNAME = "neo4j";

	public static final String DEFAULT_PASSWORD = "password";
	
	@ClassRule
	public static Neo4jRule neo4jRule;
	
	protected static Driver neo4JDriver;
	
	public static Neo4JDriverWrapper driverWrapper;

	static {
		neo4jRule = new Neo4jRule().withFixture("create (i:Item {description:'an item'})");
	}

	@BeforeClass
	public static void init() {
		neo4JDriver = GraphDatabase.driver(neo4jRule.boltURI(),
				Config.build().withSessionLivenessCheckTimeout(2000).withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
	
		Map<String, String> config = new HashMap<String, String>();
		config.put(Neo4JDriverWrapper.URL, neo4jRule.boltURI().toString());
		config.put(Neo4JDriverWrapper.USERNAME_PARAM, "");
		config.put(Neo4JDriverWrapper.PASSWORD_PARAM, "");
		
		driverWrapper = new Neo4JDriverWrapperMock(config, neo4JDriver);
	}

	@AfterClass
	public static void tearDown(){
		neo4JDriver.close();
	}
}
