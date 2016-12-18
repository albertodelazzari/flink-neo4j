/**
 * 
 */
package org.apache.flink.mapping.neo4j;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.driver.v1.Statement;

/**
 * This class represents a mapping strategy that is:
 * <ul>
 * <li>a templated cypher query</li>
 * <li>a set of parameters (that is a set of key-value pairs) that will be used
 * in the query</li>
 * </ul>
 * 
 * @author Alberto De Lazzari
 *
 */
public class Neo4JSinkMappingStrategy<T, M extends DeserializationMapper<T>> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The templated cypher query that will be executed on Neo4J
	 */
	private String templateStatement;

	/**
	 * The mapper that will map a item from a Flink stream to a set of
	 * parameters
	 * 
	 * @see DeserializationMapper
	 */
	private M mapper;

	/**
	 * 
	 * @param templateStatement the cypher statement template
	 * @param mapper an actual DeerializationMapper implementation
	 */
	public Neo4JSinkMappingStrategy(String templateStatement, M mapper) {
		this.templateStatement = templateStatement;
		this.mapper = mapper;
	}

	/**
	 * Generate a statement with parameters (templated cypher query) for a given
	 * item and a convert function.
	 * 
	 * @param item a data stream item
	 * @return the executable statement with its text and parameters
	 */
	public Statement getStatement(T item) {
		Statement statement = new Statement(templateStatement);

		Map<String, Object> parameters = mapper.deserialize(item);
		return statement.withParameters(parameters);
	}
}
