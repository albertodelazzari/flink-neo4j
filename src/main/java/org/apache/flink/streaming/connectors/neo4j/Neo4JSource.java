/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JSourceMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.RecordMapper;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSource<T> extends RichSourceFunction<T> {

	private static final long serialVersionUID = 1L;

	private transient Neo4JDriverWrapper driver;

	/**
	 * The mapping strategy that we use to map data from Flink to Neo4J
	 * 
	 * @see Neo4JMappingStrategy
	 */
	private Neo4JSourceMappingStrategy<T, RecordMapper<T>> sourceMappingStrategy;

	private Map<String, String> config;

	public Neo4JSource(final Map<String, String> config) {
		this.config = config;
	}

	/**
	 * Initialize the connection to Neo4J. As the Elasticsearch connector we can
	 * use and embedded instance or a cluster
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		driver = new Neo4JDriverWrapper(this.config);
	}

	@Override
	public void cancel() {
		driver.close();
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		Statement queryStatement = sourceMappingStrategy.getStatement();
		Session session = driver.session();

		// We should use a statement with parameters
		StatementResult result = session.run(queryStatement);
		while (result.hasNext()) {
			Record record = result.next();
			
			T item = sourceMappingStrategy.map(record);
			sourceContext.collect(item);
		}
	}
}
