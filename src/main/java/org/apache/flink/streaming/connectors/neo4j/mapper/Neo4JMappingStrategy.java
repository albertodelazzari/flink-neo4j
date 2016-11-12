/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.driver.v1.Statement;

/**
 * @author Alberto De Lazzari
 *
 */
public class Neo4JMappingStrategy<T, Mapper extends ValuesMapper<T>> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String templateStatement;

	private Mapper mapper;

	/**
	 * 
	 * @param templateStatement
	 * @param mapper
	 */
	public Neo4JMappingStrategy(String templateStatement, Mapper mapper) {
		this.templateStatement = templateStatement;
		this.mapper = mapper;
	}

	/**
	 * Generate a statement with parameters for a given item and a convert
	 * function
	 * 
	 * @return
	 */
	public Statement getStatement(T item) {
		Statement statement = new Statement(templateStatement);

		Map<String, Object> parameters = mapper.convert(item);
		return statement.withParameters(parameters);
	}
}
