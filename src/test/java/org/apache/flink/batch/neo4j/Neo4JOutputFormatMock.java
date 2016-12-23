/**
 * 
 */
package org.apache.flink.batch.neo4j;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.embedded.neo4j.Neo4JBaseEmbeddedTest;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JOutputFormatMock<T> extends Neo4JOutputFormat<T> {

	private static final long serialVersionUID = 1L;

	public Neo4JOutputFormatMock(Neo4JDeserializationMappingStrategy<T, DeserializationMapper<T>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		// We use a static driver wrapper with an embedded Neo4J instance
		driver = Neo4JBaseEmbeddedTest.driverWrapper;
		session = driver.session();
	}
	
	@Override
	public void writeRecord(T record) throws IOException {
		super.writeRecord(record);
		session.run("MATCH (n) return n").list();
		driver.session().run("MATCH (n) return n").list();
	}
	
	@Override
	public void close() throws IOException {
		// The driver will be close by the test class
		if (session != null && session.isOpen()) {
			session.close();
		}

	}
}
