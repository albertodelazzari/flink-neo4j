package org.apache.flink.streaming.connectors.neo4j;

import java.io.File;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.neo4j.mapper.Neo4JMappingStrategy;
import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapper;
import org.neo4j.driver.v1.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4JSinkMock<T> extends Neo4JSink<T> {

	private transient GraphDatabaseService graphDatabaseService;

	private static final long serialVersionUID = 1L;

	public Neo4JSinkMock(Neo4JMappingStrategy<T, ValuesMapper<T>> mappingStrategy, Map<String, String> config) {
		super(mappingStrategy, null);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		GraphDatabaseBuilder builder = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(new File("target/tmp/data/graph.db"));
		graphDatabaseService = builder.newGraphDatabase();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		graphDatabaseService.shutdown();
	}

	@Override
	public void invoke(T element) throws Exception {
		Statement queryStatement = this.mappingStrategy.getStatement(element);
		graphDatabaseService.execute(queryStatement.text(), queryStatement.parameters().asMap());
	}
}
