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
	 * @param record a record (as a Map) that will be returned by the cypher query
	 * @return an item of type T that is a transformation of a record into an item T
	 */
	public T serialize(Map<String, Object> record);
	
	public Class<T> getType();
}