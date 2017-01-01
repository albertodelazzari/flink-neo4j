/**
 * 
 */
package org.apache.flink.batch.neo4j;

import java.util.Map;

import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.SerializationMapper;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JInputFormatMock<T> extends Neo4JInputFormat<T> {

	private static final long serialVersionUID = 1L;

	public Neo4JInputFormatMock(Neo4JSerializationMappingStrategy<T, SerializationMapper<T>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void openInputFormat() {
		super.openInputFormat();
		// We use a static driver wrapper with an embedded Neo4J instance
		driver = Neo4JBaseEmbeddedConfig.driverWrapper;
		session = driver.session();
	}
}
