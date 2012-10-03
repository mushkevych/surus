package com.reinvent.surus.model;

import com.reinvent.surus.mapping.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Bohdan Mushkevych
 * Description: Example data model with multi-component rowKey
 */
public class ExampleComplex {
    @HRowKey(components = {
            @HFieldComponent(name = Constants.TIMEPERIOD, length = Bytes.SIZEOF_INT, type = Integer.class),
            @HFieldComponent(name = Constants.DOMAIN_NAME, length = 64, type = String.class)
    })
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

    public ExampleComplex() {
    }
}
