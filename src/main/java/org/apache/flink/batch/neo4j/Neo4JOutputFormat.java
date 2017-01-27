/**
 * 
 */
package org.apache.flink.batch.neo4j;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.Neo4JDriverWrapper;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JOutputFormat<T> extends RichOutputFormat<T> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JOutputFormat.class);

	/**
	 * A map representing the Neo4J connection parameters
	 */
	private Map<String, String> config;

	protected transient Neo4JDriverWrapper driver;

	private Neo4JDeserializationMappingStrategy<T, DeserializationMapper<T>> mappingStrategy;

	protected transient StatementResult result;

	protected transient Session session;

	/**
	 * The default constructor
	 * 
	 * @param mappingStrategy
	 *            a mapping strategy that will be used to map batch data to
	 *            Neo4J
	 * @param config
	 *            the connection configurations for Neo4J
	 */
	public Neo4JOutputFormat(final Neo4JDeserializationMappingStrategy<T, DeserializationMapper<T>> mappingStrategy,
			final Map<String, String> config) {
		this.mappingStrategy = mappingStrategy;
		this.config = config;
	}

	@Override
	public void configure(Configuration parameters) {
		// Do nothing here
	}

	/**
	 * This method creates a Neo4J driver and establishes a session
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		driver = new Neo4JDriverWrapper(this.config);
		session = driver.session();
	}

	/**
	 * Write the record from Flink to Neo4J
	 */
	@Override
	public void writeRecord(T record) throws IOException {
		Statement statement = mappingStrategy.getStatement(record);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("running {}", statement.text());
		}
		session.run(statement);
	}

	/**
	 * This method first closes the current session and then disposes the NeoJ
	 * driver
	 * 
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		LOGGER.debug("closing current session {}", session);
		if (session != null && session.isOpen()) {
			session.close();
		}

		if (driver != null) {
			driver.close();
		}
	}
}
