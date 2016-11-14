/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.io.Serializable;
import java.util.Map;

/**
 * This class maps a generic item from a (Flink) stream to a set of key-value
 * pairs that will be used in a templated cypher query.
 * 
 * @author Alberto De Lazzari
 *
 */
public abstract class ValuesMapper<T> implements Serializable {

	/**
	 * The default serial version UID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Convert a generic item to a key-value pairs (used by a cypher statement)
	 * 
	 * @param item the data stream item
	 * @return a key-value set that will be used in a cypher statement
	 */
	public abstract Map<String, Object> convert(T item);
}