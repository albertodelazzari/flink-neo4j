package org.apache.flink.streaming.connectors.neo4j;

import java.util.Map;

import org.apache.flink.streaming.connectors.neo4j.mapper.SerializationMapper;

public class StringSerializationMapper implements SerializationMapper<String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String serialize(Map<String, Object> record) {
		return record.get("i.description").toString();
	}
}