package com.maayalee.libjava.flattenjson;

import java.util.*;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.slf4j.Logger;

public class FlattenJson {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenJson.class);

  public void addRule(String expression, String flatten) {
    FlattenRule rule = new FlattenRule();
    rule.express = expression;
    rule.flatten = flatten;
    rules.add(rule);
  }

  public JsonObject unnest(JsonObject object) {
    flattenObject = new JsonObject();
    extractObjects = new HashMap<String, Map<Integer, JsonObject>>();

    String path = "$";
    // 1. unnest 처리후 원본 데이터에서 해당 필드를 제거한다. 이대 unnest 대상은 flattenObject['xxxx']에 넣고 리스트로 따로 뺄 객체는
    // flattenObject[list_name]에 넣는다.
    retrieveFlatten(path, object);
    retrieveList(path, object, -1);
    // 2. 나머지 필드는 그대로 복사. 모든 flattenObject에 복사한다.
    retrieveElements(path, object, flattenObject);
    for (String extractName : extractObjects.keySet()) {
      Map<Integer, JsonObject> rows = extractObjects.get(extractName);
      for (Integer rowIndex : rows.keySet()) {
        retrieveElements(path, object, rows.get(rowIndex));
      }
    }
    return flattenObject;
  }

  public Map<Integer, JsonObject> getExtractRows(String listName) {
    return extractObjects.get(listName);
  }

  private void retrieveFlatten(String path, JsonObject object) {
    Set<Entry<String, JsonElement>> entrySet = object.entrySet();
    for (Entry<String, JsonElement> entry : entrySet ) {
      String key = entry.getKey();
      JsonElement value = entry.getValue();
      String currentElementPath = path + "." + key;

      // LOG.info("Key:" + key + ", Value:" + value + ", Path:" + currentElementPath);

      for (FlattenRule rule : rules) {
        // LOG.info("rule:" + rule.express);
        if (rule.express.equals(currentElementPath)) {
          // LOG.info("Path:" + currentElementPath + ", Rule:" + rule.express + " => " + rule.flatten);
          insertObject(flattenObject, rule.flatten, value);
          object.remove(key);
        }
      }
      if (value instanceof JsonObject) {
        retrieveFlatten(currentElementPath, (JsonObject)value);

        // 자식의 순회 처리가 끝날때마다 키를 가지지 않는 필드는 제거해준다.
        JsonObject postValue = (JsonObject) value;
        if (postValue.size() == 0) {
          object.remove(key);
        }
      }
    }
  }

  private void retrieveList(String path, JsonObject object, int index) {
    List<String> keyList = new ArrayList<String>(object.keySet());
    Set<Entry<String, JsonElement>> entrySet = object.entrySet();
    for (int i  = 0; i < keyList.size(); ++i) {
      String key = keyList.get(i);
      JsonElement value = object.get(key);
      String currentElementPath = path + "." + key;

      // LOG.info("Key:" + key + ", Value:" + value + ", Path:" + currentElementPath);

      for (FlattenRule rule : rules) {
        // LOG.info("rule:" + rule.express);
        if (rule.express.equals(currentElementPath) && index != -1) {
          // LOG.info("Path:" + currentElementPath + ", Rule:" + rule.express + " => " + rule.flatten + ", Row Index: "
          // + index);

          String[] flattenTokens = rule.flatten.split("\\.");
          String listName = flattenTokens[0];
          Map<Integer, JsonObject> rows;
          if (extractObjects.containsKey(listName)) {
            rows = extractObjects.get(listName);
          } else {
            rows = new HashMap<Integer, JsonObject>();
            extractObjects.put(listName, rows);
          }
          JsonObject row;
          if (rows.containsKey(index)) {
            row = rows.get(index);
          } else {
            row = new JsonObject();
            if (useExtractIndex) {
              row.add("extract_index", new JsonPrimitive(index));
            }
            rows.put(index, row);
          }
          insertObject(row, rule.flatten.replace(listName, "$"), value);
          object.remove(key);
        }
      }

      if (value instanceof JsonObject) {
        retrieveList(currentElementPath, (JsonObject) value, -1);
        // 자식의 순회 처리가 끝날때마다 키를 가지지 않는 필드는 제거해준다.
        JsonObject postValue = (JsonObject) value;
        if (postValue.size() == 0) {
          object.remove(key);
        }
      }

      if (value instanceof JsonArray) {
        currentElementPath += "[]";
        JsonArray childs = (JsonArray) value;
        for (int j = 0; j < childs.size(); ++j) {
          Object child = childs.get(j);
          if (child instanceof JsonObject) {
            retrieveList(currentElementPath, (JsonObject) child, j);
          }
        }

        List<JsonElement> removeList = new LinkedList<JsonElement>();
        for (int j = 0; j < childs.size(); ++j) {
          Object child = childs.get(j);
          if (child instanceof JsonObject) {
            JsonObject childValue = (JsonObject) child;
            if (childValue.size() == 0) {
              removeList.add(childValue);
            }
          }
        }
        for (int j = 0; j < removeList.size(); ++j) {
          childs.remove(removeList.get(j));
        }
        if (childs.size() == 0) {
          object.remove(key);
        }
      }
    }
  }

  private void retrieveElements(String path, JsonObject object, JsonObject writeObject) {
    Set<Entry<String, JsonElement>> entrySet = object.entrySet();
    for (Entry<String, JsonElement> entry : entrySet ) {
      String key = entry.getKey();
      JsonElement value = entry.getValue();
      String currentElementPath = path + "." + key;

      // LOG.info("Key:" + key + ", Value:" + value + ", Path:" + currentElementPath);
      insertObject(writeObject, currentElementPath, value);
      if (value instanceof JsonObject) {
        retrieveElements(currentElementPath, (JsonObject) value, writeObject);
      }
    }
  }

  private void insertObject(JsonObject rootObject, String flatten, JsonElement insertValue) {
    String[] tokens = flatten.split("\\.");
    String insertKey = tokens[tokens.length - 1];
    String matchPath = "";
    for (int i = 0; i < tokens.length; ++i) {
      matchPath += tokens[i];
      if (i != (tokens.length - 1)) {
        matchPath += ".";
      }
    }

    String debugTokenString = "";
    for (int i = 0; i < tokens.length; ++i) {
      debugTokenString += tokens[i];
      debugTokenString += ", ";
    }
    // LOG.info("MatchPath: " + matchPath + " / Tokens: " + debugTokenString);

    String path = "$";
    JsonObject currentObject = rootObject;
    for (int i = 1; i < tokens.length; ++i) {
      String key = tokens[i];
      path = path + "." + key;
      if (path.equals(matchPath)) {
        // LOG.info("Match Insert Key: " + insertKey);
        currentObject.add(insertKey, insertValue);
        return;
      } else {
        JsonObject middleObject;
        if (currentObject.has(key)) {
          middleObject = (JsonObject) currentObject.get(key);
        } else {
          middleObject = new JsonObject();
          // LOG.info("Insert Key: " + key);
          currentObject.add(key, middleObject);
        }
        currentObject = middleObject;
      }
    }
  }

  public void toogleExtractIndex(boolean value)  {
    this.useExtractIndex = value;
  }

  private JsonObject flattenObject;
  private Map<String, Map<Integer, JsonObject>> extractObjects;
  private List<FlattenRule> rules = new LinkedList<FlattenRule>();
  private boolean useExtractIndex = false;
}
