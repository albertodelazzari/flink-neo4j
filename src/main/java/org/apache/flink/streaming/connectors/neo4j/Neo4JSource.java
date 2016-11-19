/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.neo4j.driver.v1.Statement;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSource<T> extends RichSourceFunction<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Neo4JDriverWrapper driver;

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

	}

	@Override
	public void run(SourceContext<T> arg0) throws Exception {

		Statement statement = new Statement("");
		driver.session().run(statement);
	}
}
