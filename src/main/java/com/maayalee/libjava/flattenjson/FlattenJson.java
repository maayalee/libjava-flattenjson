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
        extractObjects = new HashMap<String, Map<Integer, Map<String, Object>>>();

        String path = "$";
        // 1. unnest 처리후 원본 데이터에서 해당 필드를 제거한다. 이대 unnest 대상은 flattenObject['xxxx']에 넣고 리스트로 따로 뺄 객체는 flattenObject[list_name]에 넣는다.
        retrieveFlatten(path, object);
        retrieveList(path, object, -1);
        // 2. 나머지 필드는 그대로 복사. 모든 flattenObject에 복사한다.
        retrieveElements(path, object, flattenObject);
        for (String extractName : extractObjects.keySet()) {
            Map<Integer, Map<String, Object>> rows = extractObjects.get(extractName);
            for (Integer rowIndex : rows.keySet()) {
                retrieveElements(path, object, rows.get(rowIndex));
            }
        }

        // 다른 테이블로 쪼개는 룰의 경우 다른 HashMap에 구성한다.
        LOG.info("Output flattenObject:" + flattenObject.toString());
        for (String extractName : extractObjects.keySet()) {
            LOG.info("Output extractName: " + extractName);
            Map<Integer, Map<String, Object>> rows = extractObjects.get(extractName);
            for (Integer rowIndex : rows.keySet()) {
                LOG.info("Output rowIndex:" + rows.get(rowIndex).toString());
            }
        }
        return flattenObject;
    }

    public Map<Integer, Map<String, Object>> getExtractRows(String listName) {
        return extractObjects.get(listName);
    }

    private void retrieveFlatten(String path, Map<String, Object> object) {
        List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>(object.entrySet());
        for (int i = 0; i < entries.size(); ++i) {
            Map.Entry<String, Object> entry =  entries.get(i);
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentElementPath = path + "." + key;

            LOG.info("Key:" + key +  ", Value:" + value + ", Path:" + currentElementPath);

            for (FlattenRule rule : rules) {
                LOG.info("rule:" + rule.express);
                if (rule.express.equals(currentElementPath)) {
                    LOG.info("Path:" + currentElementPath + ", Rule:" + rule.express + " => " + rule.flatten);
                    insertObject(flattenObject, rule.flatten, value);
                    object.remove(key);
                }
            }
            if (value instanceof Map) {
                retrieveFlatten(currentElementPath, (Map<String, Object>)value);

                // 자식의 순회 처리가 끝날때마다 키를 가지지 않는 필드는 제거해준다.
                Map<String, Object> postValue = (Map<String, Object>)value;
                if (postValue.keySet().size() == 0) {
                    object.remove(key);
                }
            }
        }
    }

    private void retrieveList(String path, Map<String, Object> object, int index) {
        List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>(object.entrySet());
        for (int i = 0; i < entries.size(); ++i) {
            Map.Entry<String, Object> entry =  entries.get(i);
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentElementPath = path + "." + key;

            LOG.info("Key:" + key +  ", Value:" + value + ", Path:" + currentElementPath);

            for (FlattenRule rule : rules) {
                LOG.info("rule:" + rule.express);
                if (rule.express.equals(currentElementPath) && index != -1) {
                    LOG.info("Path:" + currentElementPath + ", Rule:" + rule.express + " => " + rule.flatten + ", Row Index: " + index);

                    String[] flattenTokens = rule.flatten.split("\\.");
                    String listName = flattenTokens[0];
                    Map<Integer, Map<String, Object>> rows;
                    if (extractObjects.containsKey(listName))  {
                        rows =  extractObjects.get(listName);
                    }
                    else {
                        rows = new HashMap<Integer, Map<String, Object>>();
                        extractObjects.put(listName, rows);
                    }
                    Map<String, Object> row;
                    if (rows.containsKey(index)) {
                        row = rows.get(index);
                    }
                    else {
                        row = new HashMap<String, Object>();
                        rows.put(index, row);
                    }
                    insertObject(row, rule.flatten.replace(listName, "$"), value);
                    object.remove(key);
                }
            }

            if (value instanceof Map) {
                retrieveList(currentElementPath, (Map<String, Object>)value, -1);
                // 자식의 순회 처리가 끝날때마다 키를 가지지 않는 필드는 제거해준다.
                Map<String, Object> postValue = (Map<String, Object>)value;
                if (postValue.keySet().size() == 0) {
                    object.remove(key);
                }
            }

            if (value instanceof List) {
                currentElementPath += "[]";
                List<Object> childs = (List<Object>)value;
                for (int j = 0; j < childs.size(); ++j) {
                    Object child = childs.get(j);
                    if (child instanceof Map) {
                        retrieveList(currentElementPath , (Map<String, Object>)child, j);
                    }
                }
                Iterator<Object> it = childs.iterator();
                while (it.hasNext()) {
                    Object child = it.next();
                    if (child instanceof Map) {
                        Map<String, Object> childValue = (Map<String, Object>)child;
                        if (childValue.size() == 0) {
                            it.remove();
                        }
                    }
                }

                if (childs.size() == 0 ) {
                    object.remove(key);
                }
            }
        }
    }

    private void retrieveElements(String path, Map<String, Object> object, Map<String, Object> writeObject) {
        List<Map.Entry<String, Object>> entries = new ArrayList<Map.Entry<String, Object>>(object.entrySet());
        for (int i = 0; i < entries.size(); ++i) {
            Map.Entry<String, Object> entry =  entries.get(i);
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentElementPath = path + "." + key;

            LOG.info("Key:" + key +  ", Value:" + value + ", Path:" + currentElementPath);
            insertObject(writeObject, currentElementPath, value);
            if (value instanceof Map) {
                retrieveElements(currentElementPath, (Map<String, Object>)value, writeObject);
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

    private Map<String, Object> flattenObject;
    private Map<String, Map<Integer, Map<String, Object>>> extractObjects;
    private List<FlattenRule> rules = new LinkedList<FlattenRule>();
}
