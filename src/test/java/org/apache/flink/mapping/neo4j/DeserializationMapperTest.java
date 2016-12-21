/**
 * 
 */
package org.apache.flink.mapping.neo4j;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Alberto De Lazzari
 *
 */
public class DeserializationMapperTest {

	@Test
	public void testStringValuesMapper() {
		StringValuesMapper stringValuesMapper = new StringValuesMapper();

		String item = "item";
		Map<String, Object> map = stringValuesMapper.deserialize(item);

		Assert.assertNotNull(map);
		Assert.assertTrue(!map.isEmpty());

		Object value = map.get("key");
		Assert.assertNotNull(value);
		Assert.assertArrayEquals(new String[] { "item" }, new String[] { value.toString() });
	}
}
