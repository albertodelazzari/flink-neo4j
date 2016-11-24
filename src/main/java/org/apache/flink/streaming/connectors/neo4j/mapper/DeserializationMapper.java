/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.util.Map;

/**
 * @author Alberto De Lazzari
 *
 */
@FunctionalInterface
public interface DeserializationMapper<T> extends Mapper<T> {

	/**
	 * Convert a generic item to a key-value pairs (used by a cypher statement)
	 * 
	 * @param item the data stream item
	 * @return a key-value set that will be used in a cypher statement
	 */
	public Map<String, Object> deserialize(T item);
}
