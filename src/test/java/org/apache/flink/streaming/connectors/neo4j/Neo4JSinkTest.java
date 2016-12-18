package org.apache.flink.streaming.connectors.neo4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JSinkMappingStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class Neo4JSinkTest implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_URL = "bolt://localhost:7687";

	private static final String DEFAULT_USERNAME = "neo4j";

	private static final String DEFAULT_PASSWORD = "password";

	private static final ArrayList<Tuple2<String, Integer>> collection = new ArrayList<>(20);

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>("neo4j-" + i, i));
		}
	}

	@Test
	public void testSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(collection);

		String statementTemplate = "MERGE (tuple:Tuple {name: {t1}, index: {t2}}) RETURN tuple";
		Map<String, String> config = new HashMap<String, String>();
		config.put(Neo4JDriverWrapper.URL, DEFAULT_URL);
		config.put(Neo4JDriverWrapper.USERNAME_PARAM, DEFAULT_USERNAME);
		config.put(Neo4JDriverWrapper.PASSWORD_PARAM, DEFAULT_PASSWORD);

		DeserializationMapper<Tuple2<String, Integer>> mapper = new SimpleValuesMapper();
		Neo4JSinkMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>> mappingStrategy = new Neo4JSinkMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>>(
				statementTemplate, mapper);

		Neo4JSinkMock<Tuple2<String, Integer>> neo4jSink = new Neo4JSinkMock<Tuple2<String, Integer>>(mappingStrategy,
				config);
		source.addSink(neo4jSink);

		env.execute();
	}

	public class SimpleValuesMapper implements DeserializationMapper<Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1681921169214823084L;

		public SimpleValuesMapper() {
			super();
		}

		@Override
		public Map<String, Object> deserialize(Tuple2<String, Integer> item) {
			HashMap<String, Object> values = new HashMap<String, Object>();

			values.put("t1", item.f1);
			values.put("t2", item.f0);

			return values;
		}

	}
}