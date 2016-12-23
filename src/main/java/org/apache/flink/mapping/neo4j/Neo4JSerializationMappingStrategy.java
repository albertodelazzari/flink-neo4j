package org.apache.flink.mapping.neo4j;

import java.io.Serializable;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

public class Neo4JSerializationMappingStrategy<T, M extends SerializationMapper<T>> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The templated cypher query that will be executed on Neo4J
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
	public Neo4JSerializationMappingStrategy(final String templateStatement, M mapper) {
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