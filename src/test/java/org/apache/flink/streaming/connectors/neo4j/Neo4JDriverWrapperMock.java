package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.neo4j.driver.v1.Driver;

public class Neo4JDriverWrapperMock extends Neo4JDriverWrapper {

	private static final long serialVersionUID = 1L;

	public Neo4JDriverWrapperMock(Map<String, String> parameters) {
		super(parameters);
	}
	
	public Neo4JDriverWrapperMock(Map<String, String> parameters, Driver driver) {
		this(parameters);
		this.driver = driver;
	}

	@Override
	protected void initDriver() {
		// Do nothing
	}
}