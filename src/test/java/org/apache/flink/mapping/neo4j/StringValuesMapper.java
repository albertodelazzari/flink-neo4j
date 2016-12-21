package org.apache.flink.mapping.neo4j;

import java.util.HashMap;
import java.util.Map;

public class StringValuesMapper implements DeserializationMapper<String> {

	private static final long serialVersionUID = 1L;

	@Override
	public Map<String, Object> deserialize(String item) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("key", item);
		return map;
	}
}