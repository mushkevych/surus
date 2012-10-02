package com.reinvent.surus.model;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Bohdan Mushkevych
 *         Description: module contains common logic for map-based operations
 */
public class ModelHelper {
    public static <K, V extends Number>
    void mergeFamilies(Map<K, V> familySource, Map<K, V> familyTarget) {
        // methods iterates thru source family and copies its entries to target family
        // in case key already exists in both families - then the values are added
        if (familySource == null) {
            return;
        }

        for (K key : familySource.keySet()) {
            if (!familyTarget.containsKey(key)) {
                familyTarget.put(key, familySource.get(key));
            } else {
                if (familyTarget.get(key).getClass() == Integer.class) {
                    Integer sum = familyTarget.get(key).intValue() + familySource.get(key).intValue();
                    familyTarget.put(key, (V) sum);
                } else {
                    Double sum = familyTarget.get(key).doubleValue() + familySource.get(key).doubleValue();
                    familyTarget.put(key, (V) sum);
                }
            }
        }
    }

    public static <K>
    void incrementFamilyField(K key, Integer increment, Map<K, Integer> familyTarget) {
        // Method increments family's property <fieldName> by <increment>
        if (key == null || increment == null) {
            return;
        }

        if (familyTarget.containsKey(key)) {
            increment += familyTarget.get(key);
        }

        familyTarget.put(key, increment);
    }

    public static <L, K, V extends Number>
    void mergeJsonFamilies(Map<L, Map<K, V>> familySource, Map<L, Map<K, V>> familyTarget) {
        // methods iterates thru source family and copies its entries to target family
        // in case key already exists in both families - then the values are added
        if (familySource == null) {
            return;
        }

        for (L key : familySource.keySet()) {
            if (!familyTarget.containsKey(key)) {
                familyTarget.put(key, familySource.get(key));
            } else {
                ModelHelper.mergeFamilies(familySource.get(key), familyTarget.get(key));
            }
        }
    }

    public static void incrementPerKey(String category, String key, int increment, Map<String, Map<String, Integer>> categoryMap) {
        Map<String, Integer> mapPerKey = categoryMap.get(category);
        if (mapPerKey == null) {
            mapPerKey = new HashMap<String, Integer>();
        }
        ModelHelper.incrementFamilyField(key, increment, mapPerKey);
        categoryMap.put(category, mapPerKey);
    }

    public static <K1, K2, V2>
    void enlistTwoLevelLeaf(Map<K1, Map<K2, V2>> map, K1 key1, K2 key2, V2 value) {
        Map<K2, V2> nestedMap = map.get(key1);
        if (nestedMap == null) {
            nestedMap = new HashMap<K2, V2>();
        }

        if (value != null) {
            nestedMap.put(key2, value);
        }
        map.put(key1, nestedMap);
    }

}
