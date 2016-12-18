/**
 * 
 */
package org.apache.flink.batch.neo4j;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.mapping.neo4j.Neo4JSourceMappingStrategy;
import org.apache.flink.mapping.neo4j.SerializationMapper;
import org.apache.flink.streaming.connectors.neo4j.Neo4JDriverWrapper;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat to read data from Neo4J and generate item of type T from object
 * of type Record.
 * 
 * @see Record
 * 
 * @author Alberto De Lazzari
 *
 */
public class Neo4JInputFormat<T> extends RichInputFormat<T, InputSplit> implements NonParallelInput {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JInputFormat.class);

	/**
	 * A map representing the Neo4J connection parameters
	 */
	private Map<String, String> config;

	protected transient Neo4JDriverWrapper driver;

	private Neo4JSourceMappingStrategy<T, SerializationMapper<T>> mappingStrategy;

	protected transient StatementResult result;

	protected transient Session session;

	/**
	 * The default constructor
	 * 
	 * @param mappingStrategy
	 *            a mapping strategy that will be used to map stream data to
	 *            Neo4J data
	 * @param config
	 *            the connection configurations for Neo4J
	 */
	public Neo4JInputFormat(final Neo4JSourceMappingStrategy<T, SerializationMapper<T>> mappingStrategy,
			final Map<String, String> config) {
		this.mappingStrategy = mappingStrategy;
		this.config = config;
	}

	/**
	 * This method override the super method. It creates a Neo4JDriverWrapper
	 * and a Session that will be used to run cypher queries
	 * 
	 * @see Neo4JDriverWrapper
	 * @see org.neo4j.driver.v1.Session
	 */
	@Override
	public void openInputFormat() {
		driver = new Neo4JDriverWrapper(this.config);
		session = driver.session();
	}

	/**
	 * This method override the super method. It closes the Driver disposing all
	 * the resources
	 * 
	 * @see org.neo4j.driver.v1.Driver
	 */
	@Override
	public void closeInputFormat() {
		if (driver != null) {
			driver.close();
		}
	}

	@Override
	public void configure(Configuration parameters) {
		// Do nothing here
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return new GenericInputSplit[] { new GenericInputSplit(0, 1) };
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	/**
	 * This method override the super method. It runs the query statement
	 * defined by the mapping strategy and get the result
	 */
	@Override
	public void open(InputSplit split) throws IOException {
		Statement queryStatement = mappingStrategy.getStatement();
		LOGGER.debug("running statement {}", queryStatement.text());
		result = session.run(queryStatement);
	}

	/**
	 * This method returns true if the result has no more elements.
	 * 
	 * @see StatementResult
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return !result.hasNext();
	}

	/**
	 * Gets the next record from the result returned by the cypher query. The
	 * record will be mapped according to the mapping strategy
	 * 
	 */
	@Override
	public T nextRecord(T reuse) throws IOException {
		Record record = result.next();
		LOGGER.debug("record: {}", record);
		T item = mappingStrategy.map(record);
		LOGGER.debug("returning mapped record: {}", item);

		return item;
	}

	/**
	 * Close the Neo4J session
	 */
	@Override
	public void close() throws IOException {
		LOGGER.debug("closing current session {}", session);
		if (session != null && session.isOpen()) {
			session.close();
		}
	}
}