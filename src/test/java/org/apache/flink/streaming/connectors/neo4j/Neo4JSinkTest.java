package org.apache.flink.streaming.connectors.neo4j;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;
import org.apache.flink.mapping.neo4j.SimpleValuesMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class Neo4JSinkTest extends Neo4JBaseEmbeddedTest {

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

		DeserializationMapper<Tuple2<String, Integer>> mapper = new SimpleValuesMapper();
		Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>> mappingStrategy = new Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>>(
				statementTemplate, mapper);

		Neo4JSinkMock<Tuple2<String, Integer>> neo4jSink = new Neo4JSinkMock<Tuple2<String, Integer>>(mappingStrategy,
				neo4JConfig);
		source.addSink(neo4jSink);

		env.execute();
	}
}