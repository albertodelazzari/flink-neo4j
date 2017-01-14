/**
 * 
 */
package org.apache.flink.batch.neo4j;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;
import org.apache.flink.mapping.neo4j.SimpleValuesMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JOutputFormatTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JOutputFormatTest.class);

	private static final ArrayList<Tuple2<String, Integer>> collection = new ArrayList<>(20);

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>("neo4j-" + i, i));
		}
	}

	@Test
	public void testOutputFormatCount() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		String statementTemplate = "MERGE (tuple:Tuple {name: {t1}, index: {t2}}) RETURN tuple";
		DeserializationMapper<Tuple2<String, Integer>> mapper = new SimpleValuesMapper();
		Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>> mappingStrategy = new Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>>(
				statementTemplate, mapper);

		Neo4JOutputFormat<Tuple2<String, Integer>> outputFormat = new Neo4JOutputFormatMock<>(mappingStrategy,
				neo4JConfig);
		DataSource<Tuple2<String, Integer>> dataSource = env.fromCollection(collection);

		dataSource.output(outputFormat);

		env.execute();
	}

	@Test
	public void testSession() throws IOException {

		String statementTemplate = "MERGE (tuple:Tuple {name: {t1}, index: {t2}}) RETURN tuple";
		DeserializationMapper<Tuple2<String, Integer>> mapper = new SimpleValuesMapper();
		Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>> mappingStrategy = new Neo4JDeserializationMappingStrategy<Tuple2<String, Integer>, DeserializationMapper<Tuple2<String, Integer>>>(
				statementTemplate, mapper);

		Neo4JOutputFormat<Tuple2<String, Integer>> outputFormat = new Neo4JOutputFormatMock<>(mappingStrategy, neo4JConfig);

		LOGGER.debug("Call open and check if the session is open");
		outputFormat.open(1, 1);
		assertTrue(outputFormat.session.isOpen());
		
		LOGGER.debug("Call close and check if the session is closed");
		outputFormat.close();
		assertTrue(!outputFormat.session.isOpen());
	}
}
