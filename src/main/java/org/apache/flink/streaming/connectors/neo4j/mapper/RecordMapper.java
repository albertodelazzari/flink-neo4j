package org.apache.flink.streaming.connectors.neo4j.mapper;

import java.util.Map;

public abstract class RecordMapper<T> implements SerializationMapper<T> {

	/**
	 * The default serial version UID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public abstract T serialize(Map<String, Object> record);
}