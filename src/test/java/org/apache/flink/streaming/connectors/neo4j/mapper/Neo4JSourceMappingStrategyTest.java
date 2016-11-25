package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.neo4j.Neo4JEmbeddedDB;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Neo4JSourceMappingStrategyTest {

	final SerializationMapperTest.TupleSerializationMapper serializationMapper = new SerializationMapperTest().new TupleSerializationMapper();

	private static GraphDatabaseService databaseService;

	//@BeforeClass
	public static void initDB() throws IOException {
		databaseService = Neo4JEmbeddedDB.startDB();

		try (Transaction tx = databaseService.beginTx()) {
			Node node = databaseService.createNode();
			node.addLabel(new Label() {

				@Override
				public String name() {
					return "Item";
				}
			});
			node.setProperty("id", new Integer(1));
			node.setProperty("description", "an item");

			tx.success();
		}
	}

	@Test
	public void testMappingStrategy() {
		String templateStatement = "MATCH (i:Item) return i.id, i.description";

		Neo4JSourceMappingStrategy<Tuple2<String, Integer>, SerializationMapperTest.TupleSerializationMapper> sourceMappingStrategy = new Neo4JSourceMappingStrategy<Tuple2<String, Integer>, SerializationMapperTest.TupleSerializationMapper>(
				templateStatement, serializationMapper);

		//Result result = databaseService.execute(sourceMappingStrategy.getStatement().text());
		//Assert.assertTrue(result.hasNext());
		Assert.assertNotNull(sourceMappingStrategy.getStatement());

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("i.id", new Integer(1));
		result.put("i.description", "an item");
		
		Record record = new RecordMock(result/*result.next()*/);
		Assert.assertNotNull(record);

		Tuple2<String, Integer> tuple = sourceMappingStrategy.map(record);
		Assert.assertNotNull(tuple);
		Assert.assertNotNull(tuple.f0);
		Assert.assertNotNull(tuple.f1);

		Assert.assertArrayEquals(new String[] { "an item" }, new String[] { tuple.f0 });
		Assert.assertArrayEquals(new Integer[] { new Integer(1) }, new Integer[] { tuple.f1 });
	}

	class RecordMock implements Record {

		private Map<String, Object> map;

		public RecordMock(Map<String, Object> map) {
			this.map = map;
		}

		@Override
		public List<String> keys() {
			return new ArrayList<String>(map.keySet());
		}

		@Override
		public List<Value> values() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean containsKey(String key) {
			return map.containsKey(key);
		}

		@Override
		public int index(String key) {
			return keys().indexOf(key);
		}

		@Override
		public Value get(String key) {
			return null;
		}

		@Override
		public Value get(int index) {
			return null;
		}

		@Override
		public int size() {
			return map.size();
		}

		@Override
		public Map<String, Object> asMap() {
			return this.map;
		}

		@Override
		public <T> Map<String, T> asMap(Function<Value, T> mapper) {
			return null;
		}

		@Override
		public List<Pair<String, Value>> fields() {
			return null;
		}
	}
}