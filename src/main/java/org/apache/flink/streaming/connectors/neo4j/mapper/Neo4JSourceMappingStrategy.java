package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.Serializable;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

public class Neo4JSourceMappingStrategy<T, M extends SerializationMapper<T>> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	private String templateStatement;

	/**
	 * 
	 */
	private M mapper;

	/**
	 * 
	 * @param templateStatement the cypher statement template
	 * @param mapper an actual SerializationMapper implementation
	 */
	public Neo4JSourceMappingStrategy(final String templateStatement, M mapper) {
		this.templateStatement = templateStatement;
		this.mapper = mapper;
	}

	/**
	 * 
	 * @return the cypher statement template
	 */
	public Statement getStatement() {
		return new Statement(templateStatement);
	}

	/**
	 * Maps a single result record into a T object
	 * 
	 * @param record a record that will be returned by the cypher query
	 * @return an object of type T
	 */
	public T map(Record record) {
		return this.mapper.serialize(record.asMap());
	}
}