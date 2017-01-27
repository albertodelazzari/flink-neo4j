package org.apache.flink.streaming.connectors.neo4j;

import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.Neo4JSourceMappingStrategyString;
import org.apache.flink.mapping.neo4j.SerializationMapper;
import org.apache.flink.mapping.neo4j.StringSerializationMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

public class Neo4JSourceTest extends Neo4JBaseEmbeddedConfig {

	@Test
	public void testSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SerializationMapper<String> serializationMapper = new StringSerializationMapper();
		String statement = "MATCH (i:Item) return i.description";

		Neo4JSerializationMappingStrategy<String, SerializationMapper<String>> mappingStrategy = new Neo4JSourceMappingStrategyString(
				statement, serializationMapper);

		Neo4JSourceMock<String> sourceMock = new Neo4JSourceMock<String>(mappingStrategy, neo4JConfig);
		DataStreamSource<String> dataStreamSource = env.addSource(sourceMock, BasicTypeInfo.STRING_TYPE_INFO);

		List<String> descriptions = Lists.newArrayList();
		dataStreamSource.writeUsingOutputFormat(new LocalCollectionOutputFormat<>(descriptions));

		env.execute();

		Assert.assertTrue(descriptions.size() == 1);
		Assert.assertTrue(descriptions.get(0).equals("an item"));
	}
}
