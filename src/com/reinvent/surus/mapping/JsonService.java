package com.reinvent.surus.mapping;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.reinvent.surus.model.Constants;
import com.reinvent.surus.primarykey.AbstractPrimaryKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * Description: module takes care of serialization and deserialization of Data Model from Python JSON stream
 */
public class JsonService<T> {
	private static Logger log = Logger.getLogger(JsonService.class.getName());
    private Gson gson = new Gson();
    private JsonParser jParser = new JsonParser();
    private Class<T> clazz;
    private AbstractPrimaryKey primaryKey;

    public JsonService(Class<T> clazz, AbstractPrimaryKey primaryKey) {
        this.clazz = clazz;
        this.primaryKey = primaryKey;
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
        } else if (EntityService.isMapTypeSupported(type)
                || EntityService.isListTypeSupported(type)) {
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
        } else if (type == Boolean.class || type == Boolean.TYPE) {
            return jElement.getAsBoolean();
        } else if (type == Map.class) {
            // must be handled by specifying additionally HNestedMap annotation
            throw new IllegalArgumentException("HNestedMap or HMapProperties annotation is missing");
        } else {
            // not handled
            throw new IllegalArgumentException(String.format("Unsupported type %s for conversion", type.toString()));
        }
    }

    /**
     * method converts raw string into Java Primitive accordingly to <type>
     * @param raw string presenting the object
     * @param type of the target object
     * @return null if raw is null or valid object otherwise
     */
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
        } else if (type == Boolean.class || type == Boolean.TYPE) {
            return Boolean.valueOf(raw);
        } else if (type == Map.class) {
            // must be handled by specifying additionally HNestedMap annotation
            throw new IllegalArgumentException("Map.class is not valid Key type");
        } else {
            // not handled
            throw new IllegalArgumentException(String.format("Unsupported type %s for conversion", type.toString()));
        }
    }

    /**
     * method is parsing <column family> into Java HashMap
     * @param joFamily Json element presenting <column family>
     * @param annotation of the <column family>
     * @return null if joFamily is null or filled HashMap otherwise
     */
    private Map parseMapFamily(JsonObject joFamily, HMapFamily annotation) {
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

    /**
     * method parses JSON object and tries to read complex map from it
     * (i.e. value in the map are maps itself)
     * @param joFamily presenting <column family>
     * @param annotation from synergy model describing outer map
     * @param nestedMap annotation from synergy model describing embedded map
     * @return Map in format <key: <key: value>>, where value is a primitive
     */
    private Map parseComplexMap(JsonObject joFamily, HMapFamily annotation, HNestedMap nestedMap) {
        if (joFamily == null) {
            return null;
        }

        Type type = EntityService.getMapType(nestedMap.keyType(), nestedMap.valueType());
        Map result = new HashMap();
        for (Map.Entry<String, JsonElement> pairs : joFamily.entrySet()) {
            Object key = convertFromString(pairs.getKey(), annotation.keyType());
            Object value = convertFromElement(pairs.getValue(), type);
            result.put(key, value);
        }
        return result;
    }

    /**
     * method parses JSON string to the valid Synergy Model
     * @param json JSON String
     * @return null if json is null or valid Synergy Model instance
     */
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
                    byte[] pk;
                    Map<String, Object> keyComponentValues = new HashMap<String, Object>();

                    HFieldComponent[] keyComponents = primaryKey.getComponents();
                    for (HFieldComponent component : keyComponents) {
                        JsonObject joFamily = jsonObject.getAsJsonObject(Constants.FAMILY_STAT);
                        Object value = convertFromElement(joFamily.get(component.name()), component.type());
                        keyComponentValues.put(component.name(), value);
                    }

                    pk = primaryKey.generateRowKey(keyComponentValues).get();
                    f.set(instance, pk);

                } else if (f.isAnnotationPresent(HMapFamily.class)) {
                    HMapFamily annotation = f.getAnnotation(HMapFamily.class);
                    JsonObject joFamily = jsonObject.getAsJsonObject(annotation.family());

                    if (joFamily != null) {
                        Map parsed;
                        if (f.isAnnotationPresent(HNestedMap.class)) {
                            HNestedMap nestedMap = f.getAnnotation(HNestedMap.class);
                            parsed = parseComplexMap(joFamily, annotation, nestedMap);
                        } else {
                            parsed = parseMapFamily(joFamily, annotation);
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
                } else if (f.isAnnotationPresent(HMapProperty.class)) {
                    HMapProperty annotation = f.getAnnotation(HMapProperty.class);
                    JsonObject joFamily = jsonObject.getAsJsonObject(annotation.family());
                    JsonElement joPrimitive = joFamily.get(annotation.identifier());
                    if (joPrimitive != null) {
                        Type type = EntityService.getMapType(annotation.keyType(), annotation.valueType());
                        Object parsed = convertFromElement(joPrimitive, type);
                        f.set(instance, parsed);
                    }
                } else if (f.isAnnotationPresent(HListProperty.class)) {
                    HListProperty annotation = f.getAnnotation(HListProperty.class);
                    JsonObject joFamily = jsonObject.getAsJsonObject(annotation.family());
                    JsonElement joPrimitive = joFamily.get(annotation.identifier());
                    if (joPrimitive != null) {
                        Type type = EntityService.getListType(annotation.elementType());
                        Object parsed = convertFromElement(joPrimitive, type);
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

    public String toJson(Map instance) {
        return gson.toJson(instance);
    }

    public Gson getGson() {
        return gson;
    }
}
