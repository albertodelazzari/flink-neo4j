package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JSinkMappingStrategy;

public class Neo4JSinkMock<T> extends Neo4JSink<T> {

	private static final long serialVersionUID = 1L;

	public Neo4JSinkMock(Neo4JSinkMappingStrategy<T, DeserializationMapper<T>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// We use a static driver wrapper with an embedded Neo4J instance
		driver = Neo4JBaseEmbeddedTest.driverWrapper;
	}
}