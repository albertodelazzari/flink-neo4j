package org.apache.flink.streaming.connectors.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JSinkMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapper;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;

public class Neo4JSinkMock<T> extends Neo4JSink<T> {

	private static final long serialVersionUID = 1L;

	private Neo4JDriverWrapper driver;

	public Neo4JSinkMock(Neo4JSinkMappingStrategy<T, ValuesMapper<T>> mappingStrategy, Map<String, String> config) {
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

	@Override
	public void close() {
		driver.close();
	}

	@Override
	public void invoke(T element) throws Exception {
		Statement queryStatement = this.getMappingStrategy().getStatement(element);
		Session session = driver.session();

		session.run(queryStatement);
	}
}
