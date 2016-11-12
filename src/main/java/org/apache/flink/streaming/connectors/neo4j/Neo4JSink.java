/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapper;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSink<T> extends RichSinkFunction<T> {

	private static final long serialVersionUID = 1L;

	private transient static final Logger LOGGER = LoggerFactory.getLogger(Neo4JSink.class);

	/**
	 * Neo4J driver should be not serialized
	 */
	private transient Driver driver;

	/**
	 * The mapping strategy that we use to map data from Flink to Neo4J
	 * @see Neo4JMappingStrategy
	 */
	private Neo4JMappingStrategy<T, ValuesMapper<T>> mappingStrategy;

	/**
	 * A map representing the Neo4J connection parameters 
	 */
	private Map<String, String> config;

	/**
	 * 
	 * @param mappingStrategy
	 * @param statementTemplate
	 * @param config
	 */
	public Neo4JSink(Neo4JMappingStrategy<T, ValuesMapper<T>> mappingStrategy, final Map<String, String> config) {
		this.mappingStrategy = mappingStrategy;
		this.config = config;
	}

	/**
	 * Initialize the connection to Neo4J. As the Elasticsearch connector we can
	 * use and embedded instance or a cluster
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		driver = ConnectionFactory.getDriver(this.config);
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
	 */
	@Override
	public void invoke(T element) throws Exception {
		Statement queryStatement = this.mappingStrategy.getStatement(element);
		StatementResult result = driver.session().run(queryStatement);

		LOGGER.debug("{}", result.keys());
	}
}