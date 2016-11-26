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

	public static final String USERNAME_PARAM = "neo4j.auth.username";

	public static final String PASSWORD_PARAM = "neo4j.auth.password";

	public static final String URL = "neo4j.url";

	private Map<String, String> parameters;
	
	protected transient Driver driver;

	/**
	 * The defaut constructor.
	 * Always pass the connection url, username and password
	 * 
	 * @param parameters the connection parameters
	 */
	public Neo4JDriverWrapper(final Map<String, String> parameters) {
		// We want to ensure that all the mandatory parameters are defined
		boolean urlDefined = parameters.containsKey(URL);
		assert urlDefined;
		boolean usernameDefined = parameters.containsKey(USERNAME_PARAM);
		assert usernameDefined;
		boolean passwordDefined = parameters.containsKey(PASSWORD_PARAM);
		assert passwordDefined;
		
		this.parameters = parameters;
		this.initDriver();
	}

	/**
	 * Init the internal Neo4J driver
	 * @see org.neo4j.driver.v1.Driver
	 */
	protected void initDriver() {
		String url = parameters.get(URL);
		String username = parameters.get(USERNAME_PARAM);
		String password = parameters.get(PASSWORD_PARAM);

		AuthToken authToken = AuthTokens.basic(username, password);
		LOGGER.debug("Basic authentication token with username {}", username);

		Config config = Config.build().withEncryptionLevel(EncryptionLevel.NONE).toConfig();
		driver = GraphDatabase.driver(url, authToken, config);
		LOGGER.debug("A driver has been created to {}", url);
	}

	/**
	 * Establish a session
	 * @see org.neo4j.driver.v1.Session
	 * 
	 * @return a Neo4J session
	 */
	public Session session() {
		return driver.session();
	}
	
	/**
	 * Close all the resources assigned to the internal Neo4J Driver
	 * @see org.neo4j.driver.v1.Driver
	 */
	public void close(){
		driver.close();
	}
}