package org.apache.flink.mapping.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.mapping.neo4j.Neo4JSourceMappingStrategy;
import org.apache.flink.mapping.neo4j.SerializationMapperTest.TupleSerializationMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.v1.Record;

public class Neo4JSourceMappingStrategyTest {

	final SerializationMapperTest.TupleSerializationMapper serializationMapper = new SerializationMapperTest().new TupleSerializationMapper();

	class Neo4JSourceMappingStrategyTuple extends
			Neo4JSourceMappingStrategy<Tuple2<String, Integer>, SerializationMapperTest.TupleSerializationMapper> {

		private static final long serialVersionUID = 1L;

		public Neo4JSourceMappingStrategyTuple(String templateStatement, TupleSerializationMapper mapper) {
			super(templateStatement, mapper);
		}
	}

	@Test
	public void testMappingStrategy() {
		String templateStatement = "MATCH (i:Item) return i.id, i.description";

		Neo4JSourceMappingStrategy<Tuple2<String, Integer>, SerializationMapperTest.TupleSerializationMapper> sourceMappingStrategy = new Neo4JSourceMappingStrategyTuple(
				templateStatement, serializationMapper);

		Assert.assertNotNull(sourceMappingStrategy.getStatement());

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("i.id", new Integer(1));
		result.put("i.description", "an item");

		Record record = Mockito.mock(Record.class);
		Mockito.when(record.asMap()).thenAnswer(new Answer<Map<String, Object>>() {

			@Override
			public Map<String, Object> answer(InvocationOnMock invocation) throws Throwable {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("i.id", new Integer(1));
				map.put("i.description", "an item");
				return map;
			}
		});

		Tuple2<String, Integer> tuple = sourceMappingStrategy.map(record);
		Assert.assertNotNull(tuple);
		Assert.assertNotNull(tuple.f0);
		Assert.assertNotNull(tuple.f1);

		Assert.assertArrayEquals(new String[] { "an item" }, new String[] { tuple.f0 });
		Assert.assertArrayEquals(new Integer[] { new Integer(1) }, new Integer[] { tuple.f1 });
	}
}