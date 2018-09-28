package com.maayalee.libjava.flattenjson;
import com.google.gson.JsonParser;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

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

  public void testFlattenObject() {
    String jsonString = "{\"field1\":\"value1\",\"field2\":{\"sub1\":\"value2\",\"sub2\":{\"sub3\":\"value3\"}}, \"field3\":\"value4\"}";
    JsonParser parser = new JsonParser();
    JsonObject element = parser.parse(jsonString).getAsJsonObject();

    FlattenJson flatten = new FlattenJson();
    flatten.addRule("$.field2.sub1", "$.field2_sub1");
    flatten.addRule("$.field2.sub2.sub3", "$.field2_sub2_sub3");
    JsonObject unnestRow = flatten.unnest(element);

    assertTrue(unnestRow.get("field1").getAsString().equals("value1"));
    assertTrue(unnestRow.get("field2_sub1").getAsString().equals("value2"));
    assertTrue(unnestRow.get("field2_sub2_sub3").getAsString().equals("value3"));
  }

  public void testInsensitiveComparison() {
    String jsonString = "{\"field1\":\"value1\",\"field2\":{\"sub1\":\"value2\",\"sub2\":{\"sub3\":\"value3\"}}, \"field3\":\"value4\"}";
    JsonParser parser = new JsonParser();
    FlattenJson flatten = new FlattenJson();
    flatten.addRule("$.field2.sub1", "$.field2_sub1");
    flatten.addRule("$.field2.sub2.sub3", "$.field2_sub2_sub3");
    JsonObject unnestRow = flatten.unnest(parser.parse(jsonString).getAsJsonObject());
    LOG.info(unnestRow.toString());
    assertTrue(unnestRow.get("field1").getAsString().equals("value1"));
    assertTrue(unnestRow.get("field2_sub1").getAsString().equals("value2"));
    assertTrue(unnestRow.get("field2_sub2_sub3").getAsString().equals("value3"));

    flatten = new FlattenJson();
    flatten.toggleInsensitiveExpression(true);
    flatten.addRule("$.field2.suB1", "$.field2_sub1");
    flatten.addRule("$.field2.Sub2.SUB3", "$.field2_sub2_sub3");
    unnestRow = flatten.unnest(parser.parse(jsonString).getAsJsonObject());
    LOG.info("Caseinsensitive:" + unnestRow.toString());
    assertTrue(unnestRow.get("field1").getAsString().equals("value1"));
    assertTrue(unnestRow.get("field2_sub1").getAsString().equals("value2"));
    assertTrue(unnestRow.get("field2_sub2_sub3").getAsString().equals("value3"));
  }

  public void testFlattenArrayOfObject() {
    String jsonString = "{\"field1\":\"value1\",\"field2\":\"value2\",\"items\":[{\"id\":1,\"name\":\"item1\"},{\"id\":2,\"name\":\"item2\"},{\"id\":3,\"name\":\"item3\",\"childs\":[{\"key1\":1.1,\"key2\":2.0},{\"key1\":3.1,\"key2\":4.0}]}]}";
    JsonParser parser = new JsonParser();
    JsonObject element = parser.parse(jsonString).getAsJsonObject();

    FlattenJson flatten = new FlattenJson();
    flatten.toogleExtractIndex(true);
    flatten.addRule("$.items[].id", "extract_rows1.item_id");
    flatten.addRule("$.items[].name", "extract_rows1.item_name");
    flatten.addRule("$.items[].childs[].key1", "extract_rows2.item_child_key1");
    flatten.addRule("$.items[].childs[].key2", "extract_rows2.item_child_key2");

    JsonObject unnestRow = flatten.unnest(element);
    LOG.info(unnestRow.toString());
    assertTrue(unnestRow.get("field1").getAsString().equals("value1"));
    assertTrue(unnestRow.get("field2").getAsString().equals("value2"));
    assertFalse(unnestRow.has("items"));

    Map<Integer, JsonObject> extractRows = flatten.getExtractRows("extract_rows1");
    LOG.info(extractRows.toString());
    assertTrue(extractRows.size() == 3);
    assertTrue(extractRows.get(0).get("item_id").getAsInt() == 1);
    assertTrue(extractRows.get(0).get("item_name").getAsString().equals("item1"));
    assertTrue(extractRows.get(1).get("item_id").getAsInt() == 2);
    assertTrue(extractRows.get(1).get("item_name").getAsString().equals("item2"));
    assertTrue(extractRows.get(2).get("item_id").getAsInt() == 3);
    assertTrue(extractRows.get(2).get("item_name").getAsString().equals("item3"));

    extractRows = flatten.getExtractRows("extract_rows2");
    LOG.info(extractRows.toString());
    assertTrue(extractRows.size() == 2);
    assertTrue(extractRows.get(0).get("item_child_key1").getAsDouble() == 1.1);
    assertTrue(extractRows.get(0).get("item_child_key2").getAsDouble() == 2.0);
    assertTrue(extractRows.get(1).get("item_child_key1").getAsDouble() == 3.1);
    assertTrue(extractRows.get(1).get("item_child_key2").getAsDouble() == 4.0);
  }

  public void testFlattenArrayOfValue() {
  }
  public void testWindowTimestamp() {
    Long timestamp = 636725345856551953L / 10000000; // 9/14일
    try {
      SimpleDateFormat f = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss");
      f.setTimeZone(TimeZone.getTimeZone("UTC"));
      Date currentDate = new Date();
      Date start1 = f.parse("01-01-0001 00:00:00");
      Date start2 = f.parse("01-01-1970 00:00:00");

      GregorianCalendar cal1 = new GregorianCalendar();
      cal1.setTimeZone(TimeZone.getTimeZone("UTC"));
      cal1.setGregorianChange(new Date(Long.MIN_VALUE));
      cal1.setTime(start1);
      GregorianCalendar cal2 = new GregorianCalendar();
      cal2.setTimeZone(TimeZone.getTimeZone("UTC"));
      cal2.setGregorianChange(new Date(Long.MIN_VALUE));
      cal2.setTime(start2);
      long diff = Math.abs(cal2.getTimeInMillis() - cal1.getTimeInMillis()) / 1000;
      LOG.info("1~1970 years seconds by Cal: " + diff);

      Long seconds = (start2.getTime() - start1.getTime()) / 1000;
      LOG.info("1~1970 years seconds: " + seconds); // Calendar를 gkems Date 함수로 빼든 동일한 결과를 얻게 된다.
      Long t = timestamp - seconds;
      LOG.info("Esphper Timestampe1: " + t);
      t = timestamp - (621355968000000000L / 10000000L); // 이게 맞다. 근데 Date 함수로 계산하면 62135596800L값이 아니라 62135769600L값으로 이틀 차이가 나는 이유가 무엇인지 모르겟다. 달력 계산 사용 방식? 닷넷의 Universla Time Scale 관련 이슈라고 하는데..
      LOG.info("Esphper Timestampe2: " + t);


      seconds = (currentDate.getTime() - start1.getTime()) / 1000;
      LOG.info("Current Timestamp from 0001/1/1: " + seconds);

      seconds = (currentDate.getTime() - start2.getTime()) / 1000;
      LOG.info("Current Timestamp from 1970/1/1: " + seconds);
    }
    catch (Exception e) {
      LOG.error(e.getStackTrace().toString());
    }
  }
}
