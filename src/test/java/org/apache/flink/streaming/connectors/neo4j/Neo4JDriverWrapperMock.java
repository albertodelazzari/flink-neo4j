package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.mockito.Mockito;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class Neo4JDriverWrapperMock extends Neo4JDriverWrapper {

	private static final long serialVersionUID = 1L;

	public Neo4JDriverWrapperMock(Map<String, String> parameters) {
		super(parameters);
	}

	@Override
	protected void initDriver() {
		driver = Mockito.mock(Driver.class);

		Session session = Mockito.mock(Session.class);
		Mockito.when(driver.session()).thenReturn(session);
	}
}