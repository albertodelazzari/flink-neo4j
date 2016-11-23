/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JSinkMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapper;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSink<T> extends RichSinkFunction<T> {

	/**
	 * The default serial version UID
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JSink.class);

	/**
	 * Neo4J driver should be not serialized
	 */
	private transient Neo4JDriverWrapper driver;

	/**
	 * The mapping strategy that we use to map data from Flink to Neo4J
	 * 
	 * @see Neo4JMappingStrategy
	 */
	private Neo4JSinkMappingStrategy<T, ValuesMapper<T>> mappingStrategy;

	/**
	 * A map representing the Neo4J connection parameters
	 */
	private Map<String, String> config;

	/**
	 * The default constructor
	 * 
	 * @param mappingStrategy
	 *            a mapping strategy that will be used to map stream data to
	 *            Neo4J data
	 * @param config
	 *            the connection configurations for Neo4J
	 */
	public Neo4JSink(final Neo4JSinkMappingStrategy<T, ValuesMapper<T>> mappingStrategy, final Map<String, String> config) {
		this.mappingStrategy = mappingStrategy;
		this.config = config;
	}

	/**
	 * Initialize the connection to Neo4J. As the Elasticsearch connector we can
	 * use and embedded instance or a cluster
	 * 
	 * @param parameters
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		driver = new Neo4JDriverWrapper(this.config);
	}

	@Override
	public void close() {
		// Here we close the connection
		driver.close();
		LOGGER.debug("All the resources assigned to Neo4j driver will be closed");
	}

	/**
	 * Run a cypher query like this:
	 * 
	 * run("MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
	 * Values.parameters( "myNameParam", "Bob" ))
	 * 
	 * @param element
	 *            the data stream element
	 */
	@Override
	public void invoke(T element) throws Exception {
		Statement queryStatement = this.mappingStrategy.getStatement(element);
		Session session = driver.session();
		LOGGER.debug("running {}", queryStatement.text());
		session.run(queryStatement);

		session.close();
	}

	/**
	 * 
	 * @return
	 */
	public Neo4JSinkMappingStrategy<T, ValuesMapper<T>> getMappingStrategy() {
		return this.mappingStrategy;
	}
}