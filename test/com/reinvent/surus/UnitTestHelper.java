package com.reinvent.surus;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @author Bohdan Mushkevych
 *         Description: contains methods, common to Unit Tests
 */
public class UnitTestHelper {
    public static Result putToResult(Put put) {
        byte[] rowKey = put.getRow();
        List<KeyValue> lkv = new ArrayList<KeyValue>();
        Map<byte[], List<KeyValue>> putFamilyMap = put.getFamilyMap();
        for (Map.Entry<byte[], List<KeyValue>> entry : putFamilyMap.entrySet()) {
            List<KeyValue> columns = entry.getValue();
            for (KeyValue column : columns) {
                KeyValue keyValue = new KeyValue(rowKey,
                        column.getFamily(),
                        column.getQualifier(),
                        1L,
                        column.getValue());
                lkv.add(keyValue);
            }
        }

        KeyValue[] arrayKv = new KeyValue[lkv.size()];
        arrayKv = lkv.toArray(arrayKv);
        Arrays.sort(arrayKv, KeyValue.COMPARATOR);

        return new Result(arrayKv);
    }

    @SuppressWarnings({"unchecked"})
    public static <K, V> Map<K, V> generateMapField(Class<K> keyType, Class<V> valueType) {
        Random random = new Random(987654321L);
        Map<K, V> map = new HashMap<K, V>();
        for (int i = 0; i < 20; i++) {
            K key = null;
            if (keyType == String.class) {
                key = (K) String.valueOf(random.nextInt());
            } else if (keyType == Integer.class) {
                key = (K) Integer.valueOf(random.nextInt());
            } else if (keyType == Long.class) {
                key = (K) Long.valueOf(random.nextLong());
            }

            V value = null;
            if (valueType == String.class) {
                value = (V) String.valueOf(random.nextInt());
            } else if (valueType == Integer.class) {
                value = (V) Integer.valueOf(random.nextInt());
            } else if (valueType == Long.class) {
                value = (V) Long.valueOf(random.nextLong());
            } else if (valueType == Double.class) {
                value = (V) Double.valueOf(random.nextDouble());
            } else if (valueType == byte[].class) {
                byte[] bytes = Bytes.toBytes(random.nextInt());
                value = (V) bytes;
            }
            map.put(key, value);
        }
        return map;
    }

    @SuppressWarnings({"unchecked"})
    public static <J, K, V> Map<J, Map<K, V>> generateComplexMap(Class<J> jType, Class<K> keyType, Class<V> valueType) {
        Random random = new Random(987654321L);
        Map<J, Map<K, V>> complexMap = new HashMap<J, Map<K, V>>();
        for (int i = 0; i < 10; i++) {
            J family = null;
            if (jType == String.class) {
                family = (J) String.valueOf(random.nextInt());
            } else if (jType == Integer.class) {
                family = (J) Integer.valueOf(random.nextInt());
            } else if (jType == Long.class) {
                family = (J) Long.valueOf(random.nextLong());
            } else if (jType == Double.class) {
                family = (J) Double.valueOf(random.nextDouble());
            } else if (jType == byte[].class) {
                byte[] bytes = Bytes.toBytes(random.nextInt());
                family = (J) bytes;
            } else if (jType == Float.class) {
                family = (J) Float.valueOf(random.nextFloat());
            } else if (jType == Boolean.class) {
                family = (J) Boolean.valueOf(random.nextBoolean());
            }

            Map<K, V> map = generateMapField(keyType, valueType);
            complexMap.put(family, map);
        }
        return complexMap;
    }

    @SuppressWarnings({"unchecked"})
    public static <J, K, V> boolean equalsComplexMap(Map<J, Map<K, V>> left, Map<J, Map<K, V>> right, Class<V> valueType) {
        if (left.size() != right.size()) {
            return false;
        }

        for (Map.Entry<J, Map<K, V>> entryL : left.entrySet()) {
            if (!right.containsKey(entryL.getKey())) {
                return false;
            }

            if (!equalsMaps(right.get(entryL.getKey()), entryL.getValue(), valueType)) {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings({"unchecked"})
    public static <K, V> boolean equalsMaps(Map<K, V> left, Map<K, V> right, Class<V> valueType) {
        if (left.size() != right.size()) {
            return false;
        }

        for (Map.Entry<K, V> entryL : left.entrySet()) {
            if (!right.containsKey(entryL.getKey())) {
                return false;
            }

            if (!valueType.isArray() && !right.get(entryL.getKey()).equals(entryL.getValue())) {
                return false;
            }

            if (valueType.isArray() && valueType.getComponentType() == Byte.TYPE) {
                if (Arrays.equals((byte[]) right.get(entryL.getKey()), (byte[]) entryL.getValue()) == false) {
                    return false;
                }
            }
        }
        return true;
    }
}
