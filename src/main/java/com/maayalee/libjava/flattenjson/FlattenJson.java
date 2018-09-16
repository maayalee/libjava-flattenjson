package com.maayalee.libjava.flattenjson;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class FlattenJson {
    private static final Logger LOG = LoggerFactory.getLogger(FlattenJson.class);

    public void addRule(String expression, String flatten) {
        FlattenRule rule = new FlattenRule();
        rule.express = expression;
        rule.flatten = flatten;
        rules.add(rule);
    }

    public Map<String, Object> unnest(Map<String, Object> object) {
        flattenObject = new HashMap<String, Object>();
        String path = "$";
        retrieveElements(path, object);

        // 다른 테이블로 쪼개는 룰의 경우 다른 HashMap에 구성한다.
        LOG.info("Output 1:" + flattenObject.toString());
        return object;
    }

    private void retrieveElements(String path, Map<String, Object> object) {
        for (Map.Entry<String, Object> entry : object.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentElementPath = path + "." + key;

            LOG.info("Key:" + key +  ", Value:" + value + ", Path:" + currentElementPath);

            boolean find = false;
            for (FlattenRule rule : rules) {
                if (rule.express.equals(currentElementPath)) {
                    find = true;
                    LOG.info("Path:" + currentElementPath + ", Rule:" + rule.express + " / " + rule.flatten);
                    insertObject(flattenObject, rule.flatten, value);
                }
            }
            if (false == find) {
                insertObject(flattenObject, currentElementPath, value);
            }
            if (value instanceof Map) {
                retrieveElements(currentElementPath, (Map<String, Object>)value);
            }
        }
    }

    private void insertObject(Map<String, Object> object, String flatten, Object insertValue) {
        String[] tokens = flatten.split("\\.");
        String insertKey = tokens[tokens.length - 1];
        String matchPath = "";
        for (int i = 0; i < (tokens.length - 1); ++i ) {
            matchPath += tokens[i];
            if ( i != (tokens.length - 2)) {
                matchPath += ".";
            }
        }
        LOG.info("MatchPath: " + matchPath);

        String path = "$";
        Map<String, Object> currentObject = object;
        for (int i = 0; i < (tokens.length - 1); ++i ) {
            String key = tokens[i];
            if (path.equals(matchPath)) {
                currentObject.put(insertKey, insertValue);
            }
            else {
                Map<String, Object> insertObject = new HashMap<String, Object>();
                currentObject.put(key, insertObject);
                currentObject = insertObject;
            }
            path = path + "." + key;
        }
    }

    private Map<String, Object> flattenObject = new HashMap<String, Object>();
    private List<FlattenRule> rules = new LinkedList<FlattenRule>();
}
