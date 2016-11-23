package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.Serializable;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

public class Neo4JSourceMappingStrategy<T, MAPPER extends RecordMapper<T>> implements Serializable {

	private static final long serialVersionUID = 1L;

	private String templateStatement;

	private MAPPER mapper;

	public Neo4JSourceMappingStrategy(final String templateStatement, MAPPER mapper) {
		this.templateStatement = templateStatement;
		this.mapper = mapper;
	}

	public Statement getStatement() {
		return new Statement(templateStatement);
	}

	/**
	 * Maps a single result record into a T object
	 * 
	 * @param record
	 * @return an object of type T
	 */
	public T map(Record record) {
		return this.mapper.serialize(record.asMap());
	}
}
