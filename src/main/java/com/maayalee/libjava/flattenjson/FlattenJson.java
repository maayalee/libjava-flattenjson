package com.maayalee.libjava.flattenjson;

import java.util.*;

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
        // 1. unnest 처리후 원본 데이터에서 해당 필드를 제거한다. 이대 unnest 대상은 flattenObject['xxxx']에 넣고 리스트로 따로 뺄 객체는 flattenObject[list_name]에 넣는다.
        retrieveFlatten(path, object);
        // 2. 나머지 필드는 그대로 복사. 모든 flattenObject에 복사한다.
        retrieveElements(path, object);

        // 다른 테이블로 쪼개는 룰의 경우 다른 HashMap에 구성한다.
        LOG.info("Output 1:" + flattenObject.toString());
        return object;
    }

    private void retrieveFlatten(String path, Map<String, Object> object) {
        List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>(object.entrySet());
        for (int i = 0; i < entries.size(); ++i) {
            Map.Entry<String, Object> entry =  entries.get(i);
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
                    object.remove(key);
                }
            }
            if (value instanceof Map) {
                retrieveFlatten(currentElementPath, (Map<String, Object>)value);
            }
        }
    }

    private void retrieveElements(String path, Map<String, Object> object) {
        List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>(object.entrySet());
        for (int i = 0; i < entries.size(); ++i) {
            Map.Entry<String, Object> entry =  entries.get(i);
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentElementPath = path + "." + key;

            LOG.info("Key:" + key +  ", Value:" + value + ", Path:" + currentElementPath);
            insertObject(flattenObject, currentElementPath, value);
            if (value instanceof Map) {
                retrieveElements(currentElementPath, (Map<String, Object>)value);
            }
        }
    }

    private void insertObject(Map<String, Object> rootObject, String flatten, Object insertValue) {
        String[] tokens = flatten.split("\\.");
        String insertKey = tokens[tokens.length - 1];
        String matchPath = "";
        for (int i = 0; i < tokens.length ; ++i ) {
            matchPath += tokens[i];
            if ( i != (tokens.length - 1)) {
                matchPath += ".";
            }
        }

        String debugTokenString = "";
        for (int i = 0; i < tokens.length; ++i) {
            debugTokenString += tokens[i];
            debugTokenString += ", ";
        }
        LOG.info("MatchPath: " + matchPath + " / Tokens: " + debugTokenString);

        String path = "$";
        Map<String, Object> currentObject = rootObject;
        for (int i = 1; i < tokens.length; ++i ) {
            String key = tokens[i];
            path = path + "." + key;
            if (path.equals(matchPath)) {
                LOG.info("Match Insert Key: "  + insertKey);
                currentObject.put(insertKey, insertValue);
                return;
            }
            else {
                Map<String, Object> middleObject;
                if ( currentObject.containsKey(key)) {
                    middleObject = (Map<String, Object>)currentObject.get(key);
                }
                else {
                    middleObject = new HashMap<String, Object>();
                    LOG.info("Insert Key: "  + key);
                    currentObject.put(key, middleObject);
                }
                currentObject = middleObject;
            }
        }
    }

    private Map<String, Object> flattenObject = new HashMap<String, Object>();
    private List<FlattenRule> rules = new LinkedList<FlattenRule>();
}
