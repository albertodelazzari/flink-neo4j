package org.apache.flink.streaming.connectors.neo4j;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class Neo4JEmbeddedDB {

	private static final File DB_PATH = new File("target/neo4j-test-db");

	public static GraphDatabaseService startDB() throws IOException {
		FileUtils.deleteRecursively(DB_PATH);

		GraphDatabaseService neo4jDB = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(neo4jDB);
		
		return neo4jDB;
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
