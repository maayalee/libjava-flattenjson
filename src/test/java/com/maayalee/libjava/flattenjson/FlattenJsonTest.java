package com.maayalee.libjava.flattenjson;
import com.google.gson.JsonParser;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class FlattenJsonTest extends TestCase {

    public FlattenJsonTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(FlattenJsonTest.class);
    }

    public void testObjectFlatten() {
        String jsonString = "{\"field1\":\"test1\", \"foo\":{\"bar\":\"test2\", \"bar2\":{\"val\":\"1234\"}}}";
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> element = new Gson().fromJson(jsonString, type);

        String v = (String) element.get("field1");
        assertTrue(v.equals("test1"));
        Map<String, Object> child = (Map<String, Object>) element.get("foo");
        String v2 = (String) child.get("bar");
        assertTrue(v2.equals("test2"));

        FlattenJson flatten = new FlattenJson();
        flatten.addRule("$.foo.bar", "$.foo_bar");
        Map<String, Object> flattenElement = flatten.unnest(element);
    }
}
