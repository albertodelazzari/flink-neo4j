package org.apache.flink.batch.neo4j;

import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.Neo4JSourceMappingStrategyString;
import org.apache.flink.mapping.neo4j.SerializationMapper;
import org.apache.flink.mapping.neo4j.StringSerializationMapper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4JInputFormatTest extends Neo4JBaseEmbeddedTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JInputFormatTest.class);

	@Test
	public void testInputFormatCount() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		SerializationMapper<String> serializationMapper = new StringSerializationMapper();
		String statement = "MATCH (i:Item) return i.description";

		Neo4JSerializationMappingStrategy<String, SerializationMapper<String>> mappingStrategy = new Neo4JSourceMappingStrategyString(
				statement, serializationMapper);
		
		Neo4JInputFormatMock<String> inputFormatMock = new Neo4JInputFormatMock<>(mappingStrategy, neo4JConfig);
		
		DataSource<String> neo4jSource = env.createInput(inputFormatMock, BasicTypeInfo.STRING_TYPE_INFO);
		
		List<String> descriptions = Lists.newArrayList();
		neo4jSource.output(new LocalCollectionOutputFormat<>(descriptions));
		
		env.execute();
		
		LOGGER.debug("num of elements: {}", descriptions.size());
		Assert.assertArrayEquals(new long[]{1}, new long[]{descriptions.size()});
	}
}
