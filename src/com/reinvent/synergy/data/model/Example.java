package com.reinvent.synergy.data.model;

import com.reinvent.synergy.data.mapping.HMapFamily;
import com.reinvent.synergy.data.mapping.HMapProperty;
import com.reinvent.synergy.data.mapping.HNestedMap;
import com.reinvent.synergy.data.mapping.HProperty;
import com.reinvent.synergy.data.mapping.HRowKey;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Bohdan Mushkevych
 * Description: Example data model
 */
public class Example {
    @HRowKey
    public byte[] key;

    @HProperty(family = Constants.FAMILY_STAT, identifier = Constants.TIMEPERIOD)
    public int timePeriod;

    @HProperty(family = Constants.FAMILY_STAT, identifier = Constants.DOMAIN_NAME)
    public String domainName;

    @HMapProperty(family = Constants.FAMILY_STAT, identifier = Constants.KEYWORD_INDEX,
            keyType = String.class, valueType = Integer.class)
    public Map<String, Integer> keywordIndex = new HashMap<String, Integer>();

    @HMapFamily(family = Constants.FAMILY_KEYWORD, keyType = String.class, valueType = Integer.class)
    public Map<String, Integer> keyword = new HashMap<String, Integer>();

    @HMapFamily(family = Constants.FAMILY_OS, keyType = String.class, valueType = Map.class)
    @HNestedMap(keyType = String.class, valueType = Integer.class)
    public Map<String, Map<String, Integer>> os = new HashMap<String, Map<String, Integer>>();

    public Example() {
    }
}
