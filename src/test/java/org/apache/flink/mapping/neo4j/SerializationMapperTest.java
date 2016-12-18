package org.apache.flink.mapping.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.mapping.neo4j.SerializationMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author Alberto De Lazzari
 *
 */
public class SerializationMapperTest {

	class TupleSerializationMapper implements SerializationMapper<Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> serialize(Map<String, Object> record) {
			Tuple2<String, Integer> tuple = new Tuple2<String, Integer>();

			tuple.f0 = record.get("i.description").toString();
			tuple.f1 = Integer.valueOf(record.get("i.id").toString());

			return tuple;
		}
	}

	@Test
	public void testTuple2SerializationMapper() {
		TupleSerializationMapper serializationMapper = new TupleSerializationMapper();

		Map<String, Object> record = new HashMap<String, Object>();
		record.put("i.description", "a record");
		record.put("i.id", new Integer(10));

		Tuple2<String, Integer> tuple = serializationMapper.serialize(record);
		Assert.assertNotNull(tuple);
		Assert.assertNotNull(tuple.f0);
		Assert.assertNotNull(tuple.f1);

		Assert.assertArrayEquals(new String[] { "a record" }, new String[] { tuple.f0 });
		Assert.assertArrayEquals(new Integer[] { new Integer(10) }, new Integer[] { tuple.f1 });
	}
}
