package org.apache.flink.mapping.neo4j;

import java.util.Map;

/**
 * @author Alberto De Lazzari
 *
 */
@FunctionalInterface
public interface SerializationMapper<T> extends Mapper {
	
	/**
	 * 
	 * TODO: Maybe record should be of type org.neo4j.driver.v1.Record
	 * 
	 * @param record a record (as a Map) that will be returned by the cypher query
	 * @return an item of type T that is a transformation of a record into an item T
	 */
	public T serialize(Map<String, Object> record);
}