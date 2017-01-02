package org.apache.flink.streaming.connectors.neo4j;

import java.io.Serializable;
import java.util.Map;

import org.apache.flink.util.Preconditions;
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

	/**
	 * The username property key.
	 */
	public static final String USERNAME_PARAM = "neo4j.auth.username";

	/**
	 * The password property key.
	 */
	public static final String PASSWORD_PARAM = "neo4j.auth.password";

	/**
	 * The Neo4J server url property key.
	 */
	public static final String URL = "neo4j.url";

	/**
	 * The session liveness timeout property key.
	 */
	public static final String SESSION_LIVENESS_TIMEOUT = "neo4j.session.livetimeout";

	/**
	 * The default session liveness timeout as it's defined by the ConfigBuilder class.
	 * @see org.neo4j.driver.v1.Config.ConfigBuilder
	 */
	private static final String DEFAULT_SESSION_LIVENESS_TIMEOUT = "200";

	/**
	 * The configuration parameters.
	 */
	private Map<String, String> parameters;

	/**
	 * The internal Neo4J Driver.
	 * 
	 * @see org.neo4j.driver.v1.Driver
	 */
	protected transient Driver driver;

	/**
	 * The defaut constructor. Always pass the connection url, username and
	 * password
	 * 
	 * @param parameters
	 *            the connection parameters
	 */
	public Neo4JDriverWrapper(final Map<String, String> parameters) {
		// We want to ensure that all the mandatory parameters are defined
		boolean urlDefined = parameters.containsKey(URL);
		Preconditions.checkArgument(urlDefined, "URL connection to Neo4J server must be defined");
		
		boolean usernameDefined = parameters.containsKey(USERNAME_PARAM);
		Preconditions.checkArgument(usernameDefined, "The username must be defined");
		
		boolean passwordDefined = parameters.containsKey(PASSWORD_PARAM);
		Preconditions.checkArgument(passwordDefined, "The password must be defined");

		this.parameters = parameters;
		this.initDriver();
	}

	/**
	 * Init the internal Neo4J driver.
	 * 
	 * @see org.neo4j.driver.v1.Driver
	 */
	protected final void initDriver() {
		String url = parameters.get(URL);
		String username = parameters.get(USERNAME_PARAM);
		String password = parameters.get(PASSWORD_PARAM);
		String timeout = parameters.getOrDefault(SESSION_LIVENESS_TIMEOUT, DEFAULT_SESSION_LIVENESS_TIMEOUT);

		AuthToken authToken = AuthTokens.basic(username, password);
		LOGGER.debug("Basic authentication token with username {}", username);

		Config config = Config.build().withSessionLivenessCheckTimeout(getLongValue(timeout))
				.withEncryptionLevel(EncryptionLevel.NONE).toConfig();
		driver = GraphDatabase.driver(url, authToken, config);
		LOGGER.debug("A driver has been created to {}", url);
	}

	/**
	 * Get the corresponding long value for the given string.
	 * 
	 * @param longValue
	 *            a long number as a string
	 * @return the long value
	 */
	private long getLongValue(final String longValue) {
		return Long.valueOf(longValue);
	}

	/**
	 * Establish a session.
	 * 
	 * @see org.neo4j.driver.v1.Session
	 * @return a Neo4J session
	 */
	public final Session session() {
		return driver.session();
	}

	/**
	 * Close all the resources assigned to the internal Neo4J Driver.
	 * 
	 * @see org.neo4j.driver.v1.Driver
	 */
	public final void close() {
		driver.close();
	}
}
