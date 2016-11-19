/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Config.EncryptionLevel;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JDriverWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JDriverWrapper.class);

	public static final String USERNAME = "neo4j.auth.username";

	public static final String PASSWORD = "neo4j.auth.password";

	public static final String URL = "neo4j.url";

	private Map<String, String> parameters;
	
	private transient Driver driver;

	/**
	 * 
	 * @param parameters
	 */
	public Neo4JDriverWrapper(final Map<String, String> parameters) {
		// We want to ensure that all the mandatory parameters are defined
		assert parameters.containsKey(URL);
		assert parameters.containsKey(USERNAME);
		assert parameters.containsKey(PASSWORD);

		this.parameters = parameters;
		this.initDriver();
	}

	/**
	 * Init the internal Neo4J driver
	 * 
	 */
	protected void initDriver() {
		String url = parameters.get(URL);
		String username = parameters.get(USERNAME);
		String password = parameters.get(PASSWORD);

		AuthToken authToken = AuthTokens.basic(username, password);
		LOGGER.debug("Basic authentication token with username {}", username);

		Config config = Config.build().withEncryptionLevel(EncryptionLevel.NONE).toConfig();
		driver = GraphDatabase.driver(url, authToken, config);
		LOGGER.debug("A driver has been created to {}", url);
	}

	/**
	 * Establish a session
	 * 
	 * @return
	 */
	public Session session() {
		return driver.session();
	}
	
	/**
	 * Call all the resources assigned to the internal No4J Driver
	 * @see org.neo4j.driver.v1.Driver
	 */
	public void close(){
		driver.close();
	}
}