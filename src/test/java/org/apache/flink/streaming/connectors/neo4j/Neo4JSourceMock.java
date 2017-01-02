/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.SerializationMapper;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSourceMock<T> extends Neo4JSource<T> {

	private static final long serialVersionUID = 1L;

	public Neo4JSourceMock(final Neo4JSerializationMappingStrategy<T, SerializationMapper<T>> mappingStrategy,
			final Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// We use a static driver wrapper with an embedded Neo4J instance
		driver = Neo4JBaseEmbeddedConfig.driverWrapper;
	}
}
