/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.Serializable;
import java.util.Map;

/**
 * @author nicolacamillo
 *
 */
public abstract class ValuesMapper<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Convert a generic item to a key-value pairs (used by a cypher statement)
	 * 
	 * @param item
	 * @return
	 */
	public abstract Map<String, Object> convert(T item);
}