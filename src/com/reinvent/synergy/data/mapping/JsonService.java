package com.reinvent.synergy.data.mapping;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.reinvent.synergy.data.model.Constants;
import com.reinvent.synergy.data.system.PrimaryKey;
import com.reinvent.synergy.data.system.TimeQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * date: 02/09/11
 * Description: module takes care of serialization and deserialization of Data Model from Python Hourly JSON stream
 */
public class JsonService<T> {
	private static Logger log = Logger.getLogger(JsonService.class.getName());
    private Gson gson = new Gson();
    private JsonParser jParser = new JsonParser();
    private Class<T> clazz;

    public JsonService(Class<T> clazz) {
        this.clazz = clazz;
    }

    /**
     * Attempts to convert the value object to a byte array for storage
     * @param jElement Json Object presenting primitive
     * @param type of property object
     * @return converted object or null if input parameter is null
     * @throws IllegalArgumentException if conversion is unsupported
     */
    private Object convertFromElement(JsonElement jElement, Type type) {
        if (jElement == null) {
            return null;
        }

        if (type == Integer.class || type == Integer.TYPE) {
            return jElement.getAsInt();
        } else if (type == String.class) {
            return jElement.getAsString();
        } else if (type == EntityService.MAP_STR_STR || type == EntityService.MAP_STR_INT) {
            if (!jElement.isJsonObject()) {
                throw new IllegalArgumentException("Can not convert non JsonObject to Map");
            }
            return gson.fromJson(jElement, type);
        } else if (type == Float.class || type == Float.TYPE) {
            return jElement.getAsFloat();
        } else if (type == Double.class || type == Double.TYPE) {
            return jElement.getAsDouble();
        } else if (type == Long.class || type == Long.TYPE) {
            return jElement.getAsLong();
        } else if (type == Map.class) {
            // must be handled by specifying additionally HNestedMap annotation
            throw new IllegalArgumentException("HNestedMap annotation is missing");
        } else {
            // not handled
            throw new IllegalArgumentException(String.format("Unsupported type %s for conversion", type.toString()));
        }
    }

    private Object convertFromString(String raw, Class type) {
        if (raw == null) {
            return null;
        }

        if (type.isArray() && type.getComponentType() == Byte.TYPE) {
            return Bytes.toBytes(raw);
        } else if (type == Integer.class || type == Integer.TYPE) {
            return Integer.valueOf(raw);
        } else if (type == String.class) {
            return raw;
        } else if (type == Float.class || type == Float.TYPE) {
            return Float.valueOf(raw);
        } else if (type == Double.class || type == Double.TYPE) {
            return Double.valueOf(raw);
        } else if (type == Long.class || type == Long.TYPE) {
            return Long.valueOf(raw);
        } else if (type == Map.class) {
            // must be handled by specifying additionally HNestedMap annotation
            throw new IllegalArgumentException("Map.class is not valid Key type");
        } else {
            // not handled
            throw new IllegalArgumentException(String.format("Unsupported type %s for conversion", type.toString()));
        }
    }

    private Map parseMap(JsonObject joFamily, HMapProperty annotation) {
        if (joFamily == null) {
            return null;
        }

        Map result = new HashMap();
        for (Map.Entry<String, JsonElement> pairs : joFamily.entrySet()) {
            Object key = convertFromString(pairs.getKey(), annotation.keyType());
            Object value = convertFromElement(pairs.getValue(), annotation.valueType());
            result.put(key, value);
        }
        return result;
    }

    private Map parseComplexMap(JsonObject joFamily, HMapProperty annotation, HNestedMap nestedMap) {
        if (joFamily == null) {
            return null;
        }

        Type type = null;
        if (nestedMap.keyType() == String.class && nestedMap.valueType() == Integer.class) {
            type = EntityService.MAP_STR_INT;
        } else if (nestedMap.keyType() == String.class && nestedMap.valueType() == String.class) {
            type = EntityService.MAP_STR_STR;
        } else {
            throw new IllegalArgumentException(String.format("Unsupported key/value combination %s/%s",
                    nestedMap.keyType().getName(), nestedMap.valueType().getName()));
        }

        Map result = new HashMap();
        for (Map.Entry<String, JsonElement> pairs : joFamily.entrySet()) {
            Object key = convertFromString(pairs.getKey(), annotation.keyType());
            Object value = convertFromElement(pairs.getValue(), type);
            result.put(key, value);
        }
        return result;
    }

    public T fromJson(String json) {
        if (json == null) {
            return null;
        }

        try {
            JsonObject jsonObject = jParser.parse(json).getAsJsonObject();
            T instance = clazz.newInstance();
            Field[] fields = clazz.getDeclaredFields();
            for (Field f : fields) {
                if (f.isAnnotationPresent(HRowKey.class)) {
                    String siteName = jsonObject.get(Constants.SITE_NAME).getAsString();
                    int timePeriod = jsonObject.get(Constants.TIMESTAMP).getAsInt();
                    byte[] pk = PrimaryKey.generateKey(timePeriod, siteName, TimeQualifier.HOURLY).get();
                    f.set(instance, pk);
                } else if (f.isAnnotationPresent(HMapProperty.class)) {
                    HMapProperty annotation = f.getAnnotation(HMapProperty.class);
                    JsonObject joFamily = jsonObject.getAsJsonObject(annotation.family());

                    if (joFamily != null) {
                        Map parsed = null;
                        if (f.isAnnotationPresent(HNestedMap.class)) {
                            HNestedMap nestedMap = f.getAnnotation(HNestedMap.class);
                            parsed = parseComplexMap(joFamily, annotation, nestedMap);
                        } else {
                            parsed = parseMap(joFamily, annotation);
                        }
                        f.set(instance, parsed);
                    }
                } else if (f.isAnnotationPresent(HProperty.class)) {
                    HProperty annotation = f.getAnnotation(HProperty.class);
                    JsonObject joFamily = jsonObject.getAsJsonObject(annotation.family());
                    JsonElement joPrimitive = joFamily.get(annotation.identifier());
                    if (joPrimitive != null) {
                        Object parsed = convertFromElement(joPrimitive, f.getType());
                        f.set(instance, parsed);
                    }
                } else {
                    log.debug(String.format("Skipping field %s as it has no supported annotations", f.getName()));
                }
            }
            return instance;
        } catch (InstantiationException e) {
            log.error("Exception on parsing JSON document", e);
            return null;
        } catch (IllegalAccessException e) {
            log.error("Exception on reflection level", e);
            return null;
        }
    }
}