package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.util.Collection;

import org.apache.flink.streaming.connectors.neo4j.mapper.ValuesMapperTest.StringValuesMapper;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.v1.Statement;

public class Neo4JMappingStrategyTest {

	final ValuesMapperTest.StringValuesMapper stringValuesMapper = new ValuesMapperTest().new StringValuesMapper();

	@Test
	public void testMappingStrategy() {

		String templateStatement = "MATCH (n {id:{key}}) return n";
		Neo4JSinkMappingStrategy<String, StringValuesMapper> mappingStrategy = new Neo4JSinkMappingStrategy<String, ValuesMapperTest.StringValuesMapper>(
				templateStatement, stringValuesMapper);

		Statement statement = mappingStrategy.getStatement("dummy");
		Collection<Object> statementValues = statement.parameters().asMap().values();
		Collection<Object> mappedValues = stringValuesMapper.deserialize("dummy").values();

		Assert.assertTrue(statementValues.size() == mappedValues.size());
		Assert.assertTrue(statementValues.containsAll(mappedValues) && mappedValues.containsAll(statementValues));
	}
}
