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

    public void testObjectFlatten() {
        String jsonString = "{\"field1\":\"value1\",\"field2\":{\"sub1\":\"value2\",\"sub2\":{\"sub3\":\"value3\"}}}";
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> element = new Gson().fromJson(jsonString, type);

        String v = (String) element.get("field1");
        assertTrue(v.equals("value1"));
        Map<String, Object> child = (Map<String, Object>) element.get("field2");
        String v2 = (String) child.get("sub1");
        assertTrue(v2.equals("value2"));

        FlattenJson flatten = new FlattenJson();
        flatten.addRule("$.field2.sub1", "$.field2_sub1");
        //flatten.addRule("$.field2.sub2", "$.field2_sub2");
        Map<String, Object> flattenElement = flatten.unnest(element);

        LOG.warn("test warn");
        LOG.debug("test debug");
    }
}
