package com.maayalee.libjava.flattenjson;
import com.google.gson.JsonParser;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlattenJsonTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenJsonTest.class);

  public FlattenJsonTest(String testName) {
    super(testName);
  }

  public static Test suite() {
    return new TestSuite(FlattenJsonTest.class);
  }

  public void ttttestFlattenObject() {
    String jsonString = "{\"field1\":\"value1\",\"field2\":{\"sub1\":\"value2\",\"sub2\":{\"sub3\":\"value3\"}}, \"field3\":\"value4\"}";
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> element = new Gson().fromJson(jsonString, type);

    FlattenJson flatten = new FlattenJson();
    flatten.addRule("$.field2.sub1", "$.field2_sub1");
    flatten.addRule("$.field2.sub2.sub3", "$.field2_sub2_sub3");
    Map<String, Object> unnestRow = flatten.unnest(element);

    assertTrue(unnestRow.get("field1").equals("value1"));
    assertTrue(unnestRow.get("field2_sub1").equals("value2"));
    assertTrue(unnestRow.get("field2_sub2_sub3").equals("value3"));
  }

  public void testFlattenArrayOfObject() {
    String jsonString = "{\"field1\":\"value1\",\"field2\":\"value2\",\"items\":[{\"id\":1,\"name\":\"item1\"},{\"id\":2,\"name\":\"item2\"},{\"id\":3,\"name\":\"item3\",\"childs\":[{\"key1\":1,\"key2\":2},{\"key1\":1,\"key2\":2}]}]}";
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> element = new Gson().fromJson(jsonString, type);

    FlattenJson flatten = new FlattenJson();
    flatten.addRule("$.items[].id", "extract_rows1.item_id");
    flatten.addRule("$.items[].name", "extract_rows1.item_name");
    flatten.addRule("$.items[].childs[].key1", "extract_rows2.item_child_key1");
    flatten.addRule("$.items[].childs[].key2", "extract_rows2.item_child_key2");

    Map<String, Object> unnestRow = flatten.unnest(element);
    assertTrue(unnestRow.get("field1").equals("value1"));
    assertTrue(unnestRow.get("field2").equals("value2"));
    assertFalse(unnestRow.containsKey("items"));

    Map<Integer, Map<String, Object>> extractRows = flatten.getExtractRows("extract_rows1");
    assertTrue(extractRows.size() == 3);
    assertTrue(extractRows.get(0).get("item_id").equals(1.0));
    assertTrue(extractRows.get(0).get("item_name").equals("item1"));
    assertTrue(extractRows.get(1).get("item_id").equals(2.0));
    assertTrue(extractRows.get(1).get("item_name").equals("item2"));
    assertTrue(extractRows.get(2).get("item_id").equals(3.0));
    assertTrue(extractRows.get(2).get("item_name").equals("item3"));

    extractRows = flatten.getExtractRows("extract_rows2");
    assertTrue(extractRows.size() == 2);
    assertTrue(extractRows.get(0).get("item_child_key1").equals(1.0));
    assertTrue(extractRows.get(0).get("item_child_key2").equals(2.0));
    assertTrue(extractRows.get(1).get("item_child_key1").equals(1.0));
    assertTrue(extractRows.get(1).get("item_child_key2").equals(2.0));
  }

  public void testFlattenArrayOfValue() {
  }
}
