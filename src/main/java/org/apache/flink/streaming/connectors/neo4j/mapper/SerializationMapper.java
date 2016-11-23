package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.util.Map;

/**
 * @author Alberto De Lazzari
 *
 */
public interface SerializationMapper<T> extends Mapper<T> {
	
	/**
	 * 
	 * TODO: Maybe record should be of type org.neo4j.driver.v1.Record
	 * 
	 * @param record 
	 * @return
	 */
	public T serialize(Map<String, Object> record);
}