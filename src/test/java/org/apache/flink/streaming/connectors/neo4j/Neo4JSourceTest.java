package org.apache.flink.streaming.connectors.neo4j;

import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.Neo4JSourceMappingStrategyString;
import org.apache.flink.mapping.neo4j.SerializationMapper;
import org.apache.flink.mapping.neo4j.StringSerializationMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.junit.Test;

public class Neo4JSourceTest extends Neo4JBaseEmbeddedTest {

	@Test
	public void testSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SerializationMapper<String> serializationMapper = new StringSerializationMapper();
		String statement = "MATCH (i:Item) return i.description";

		Neo4JSerializationMappingStrategy<String, SerializationMapper<String>> mappingStrategy = new Neo4JSourceMappingStrategyString(
				statement, serializationMapper);

		Neo4JSourceMock<String> sourceMock = new Neo4JSourceMock<String>(mappingStrategy, neo4JConfig);
		DataStreamSource<String> dataStreamSource = env.addSource(sourceMock);
		dataStreamSource.addSink(new PrintSinkFunction<String>());

		env.execute();
	}
}
