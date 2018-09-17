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
    //String jsonString = "{\"field1\":\"value1\",\"field2\":\"value2\",\"items\":[{\"id\":1,\"item\":\"item1\"},{\"id\":2,\"item\":\"item2\"},{\"id\":3,\"item\":\"item3\"}]}";
    String jsonString = "{\"field1\":\"value1\",\"field2\":\"value2\",\"items\":[{\"id\":1,\"item\":\"item1\"},{\"id\":2,\"item\":\"item2\"},{\"id\":3,\"item\":\"item3\",\"childs\":[{\"key1\":1,\"key2\":2},{\"key1\":1,\"key2\":2}]}]}";
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> element = new Gson().fromJson(jsonString, type);

    FlattenJson flatten = new FlattenJson();
    flatten.addRule("$.items[].id", "extract_rows1.id");
    flatten.addRule("$.items[].item", "extract_rows1.item");
    flatten.addRule("$.items[].childs[].key1", "extract_rows2.key1");
    flatten.addRule("$.items[].childs[].key2", "extract_rows2.key2");

    Map<String, Object> unnestRow = flatten.unnest(element);
    Map<Integer, Map<String, Object>> extractRows = flatten.getExtractRows("extract_rows1");
    assertTrue(extractRows.size() == 3);
  }

  public void testFlattenArrayOfValue() {
  }
}
