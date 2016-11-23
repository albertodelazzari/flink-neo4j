/**
 * 
 */
package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.util.Map;

/**
 * @author Alberto De Lazzari
 *
 */
public interface DeserializationMapper<T> extends Mapper<T> {

	/**
	 * 
	 * @param item
	 * @return
	 */
	public Map<String, Object> deserialize(T item);
}
