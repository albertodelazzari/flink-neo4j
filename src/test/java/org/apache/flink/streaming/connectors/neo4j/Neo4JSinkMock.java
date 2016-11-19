package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapper;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;

public class Neo4JSinkMock<T> extends Neo4JSink<T> {

	private static final long serialVersionUID = 1L;

	private Neo4JDriverWrapper driver;

	public Neo4JSinkMock(Neo4JMappingStrategy<T, ValuesMapper<T>> mappingStrategy, Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		driver = new Neo4JDriverWrapperMock(this.config);
	}

	@Override
	public void close() {
		driver.close();
	}

	@Override
	public void invoke(T element) throws Exception {
		Statement queryStatement = this.mappingStrategy.getStatement(element);
		Session session = driver.session();
		
		session.run(queryStatement);
	}
}
