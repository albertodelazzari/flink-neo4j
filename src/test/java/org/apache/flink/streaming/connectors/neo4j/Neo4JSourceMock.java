/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JSourceMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.SerializationMapper;

/**
 * @author nicolacamillo
 *
 */
public class Neo4JSourceMock<T> extends Neo4JSource<T> {

	private static final long serialVersionUID = 1L;

	public Neo4JSourceMock(final Neo4JSourceMappingStrategy<T, SerializationMapper<T>> mappingStrategy, final Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Map<String, String> mockConfig = new HashMap<String, String>();
		mockConfig.put(Neo4JDriverWrapper.USERNAME_PARAM, "user");
		mockConfig.put(Neo4JDriverWrapper.PASSWORD_PARAM, "password");
		mockConfig.put(Neo4JDriverWrapper.URL, "localhost");
		driver = new Neo4JDriverWrapperMock(mockConfig);
	}
}
