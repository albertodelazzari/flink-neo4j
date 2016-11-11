/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Config.EncryptionLevel;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public final class ConnectionFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactory.class);

	public static final String USERNAME = "neo4j.auth.username";

	public static final String PASSWORD = "neo4j.auth.password";

	public static final String URL = "neo4j.url";

	public static Driver getDriver(Map<String, String> parameters) {
		// We want to ensure that all the mandatory parameters are defined
		assert parameters.containsKey(URL);
		assert parameters.containsKey(USERNAME);
		assert parameters.containsKey(PASSWORD);

		String url = parameters.get(URL);
		String username = parameters.get(USERNAME);
		String password = parameters.get(PASSWORD);

		AuthToken authToken = AuthTokens.basic(username, password);
		LOGGER.debug("Basic authentication token with username {}", username);

		Config config = Config.build().withEncryptionLevel(EncryptionLevel.NONE).toConfig();
		Driver neo4jDriver = GraphDatabase.driver(url, authToken, config);
		LOGGER.debug("A driver has been created to {}", url);

		return neo4jDriver;
	}
}