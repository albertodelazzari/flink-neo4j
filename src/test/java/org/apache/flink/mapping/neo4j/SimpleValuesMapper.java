package org.apache.flink.mapping.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.mapping.neo4j.DeserializationMapper;

public class SimpleValuesMapper implements DeserializationMapper<Tuple2<String, Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1681921169214823084L;

	public SimpleValuesMapper() {
		super();
	}

	@Override
	public Map<String, Object> deserialize(Tuple2<String, Integer> item) {
		HashMap<String, Object> values = new HashMap<String, Object>();

		values.put("t1", item.f1);
		values.put("t2", item.f0);

		return values;
	}

}