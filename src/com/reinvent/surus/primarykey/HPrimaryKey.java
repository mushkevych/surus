package com.reinvent.surus.primarykey;

import com.reinvent.surus.mapping.EntityService;
import com.reinvent.surus.mapping.HFieldComponent;
import com.reinvent.surus.mapping.HRowKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 *         Description: module contains PrimaryKey build of HRowKey & HFieldComponent
 */
public class HPrimaryKey<T> extends AbstractPrimaryKey {
    protected Class<T> clazzDataModel;
    protected EntityService<T> entityService;
    protected HFieldComponent[] components;
    protected int keyLength;
    private Logger log;


    public HPrimaryKey(Class<T> clazzDataModel, EntityService<T> entityService) {
        this.clazzDataModel = clazzDataModel;
        this.entityService = entityService;
        log = Logger.getLogger(HPrimaryKey.class.getSimpleName() + "<" + clazzDataModel.getSimpleName() + ">");

        try {
            Field[] fields = clazzDataModel.getDeclaredFields();
            for (Field f : fields) {
                if (!f.isAnnotationPresent(HRowKey.class)) {
                    // we are interested in HRowKey annotation _only_
                    continue;
                }

                HRowKey annotation = f.getAnnotation(HRowKey.class);
                components = annotation.components();

                // as there is only one HRowKey field per model
                // we can safely exit the cycle now
                break;
            }
        } catch (SecurityException e) {
            log.error("Error during HPrimaryKey initialization.", e);
        }

        validate();
        calculate_length();
    }

    /**
     * @throws IllegalArgumentException if validation fails
     */
    protected void validate() {
        // case 1: validate that key components are non null and not empty
        if (components == null || components.length == 0) {
            throw new IllegalArgumentException("HFieldComponent were not parsed successfully");
        }

        // case 2: validate that for multi-component rowKeys there are no HFieldComponent with length = -1 or 0
        if (components.length > 1) {
            for (HFieldComponent component : components) {
                if (component.length() <= 0) {
                    throw new IllegalArgumentException(String.format("Component %s keyLength is invalid %d", component.name(), component.length()));
                }
            }
        }

        // case 3: validate that single-component HRowKey has length not equal to 0 and not less than -1
        if (components.length == 1) {
            if (components[0].length() == 0 || components[0].length() < HFieldComponent.LENGTH_VARIABLE) {
                throw new IllegalArgumentException(String.format("Single-component rowKey keyLength is invalid %d", components[0].length()));
            }
        }
    }

    protected void calculate_length() {
        if (components.length == 1) {
            // special case, where primary key is constructed of 1 component
            keyLength = components[0].length();
        } else {
            for (HFieldComponent component : components) {
                keyLength += component.length();
            }
        }
    }

    /**
     * @return positive integer value or HFieldComponent.LENGTH_VARIABLE, meaning that key length is variable
     */
    @Override
    protected int getPrimaryKeyLength() {
        return keyLength;
    }

    @Override
    public HFieldComponent[] getComponents() {
        return components;
    }

    public HFieldComponent getComponent(String name) {
        for (HFieldComponent component : components) {
            if (component.name().equals(name)) {
                return component;
            }
        }

        throw new IllegalArgumentException("Component with name " + name + " can not be identified");
    }

    @Override
    public ImmutableBytesWritable generateRowKey(Map<String, Object> mapComponents) {
        if (mapComponents.size() != components.length) {
            throw new IllegalArgumentException(String.format("Number of Key Components is incorrect %d vs %d", mapComponents.size(), components.length));
        }

        byte[] primaryKey;
        if (components.length == 1) {
            Map.Entry<String, Object> entry = mapComponents.entrySet().iterator().next();

            if (keyLength == HFieldComponent.LENGTH_VARIABLE) {
                primaryKey = entityService.convertToBytes(entry.getValue());
            } else {
                primaryKey = new byte[keyLength];
                byte[] valueBytes = entityService.convertToBytes(entry.getValue());
                int localLength = Math.min(valueBytes.length, keyLength);
                Bytes.putBytes(primaryKey, 0, entityService.convertToBytes(entry.getValue()), 0, localLength);
            }
        } else {
            int offset = 0;
            primaryKey = new byte[keyLength];
            for (HFieldComponent component : components) {
                Object o = mapComponents.get(component.name());
                byte[] valueBytes = entityService.convertToBytes(o);
                int localLength = Math.min(valueBytes.length, component.length());
                Bytes.putBytes(primaryKey, offset, valueBytes, 0, localLength);
                offset += component.length();
            }
        }
        return new ImmutableBytesWritable(primaryKey);
    }

    /**
     * Parses rowKey and extracts component value from it
     *
     * @param primaryKey the
     * @return byte[] presenting component value
     */
    public byte[] getComponentValue(byte[] primaryKey, String name) {
        if (components.length == 1) {
            // for single-component primary keys, we return primary key as-is
            return primaryKey;
        }

        int offset = 0;
        int componentLength = 0;
        for (HFieldComponent component : components) {
            if (component.name().equals(name)) {
                componentLength = component.length();
                break;
            }
            offset += component.length();
        }

        byte[] bytesValue = new byte[componentLength];
        Bytes.putBytes(bytesValue, 0, primaryKey, offset, componentLength);
        return bytesValue;
    }

    /**
     * parses rowKey to select part, corresponding to the component Name
     * afterwards, it is converted accordingly to its type
     *
     * @param primaryKey the
     * @param name       of the rowKey component
     * @return casted component value
     */
    public Object getCastedComponentValue(byte[] primaryKey, String name) {
        HFieldComponent component = getComponent(name);
        byte[] bytesValue = getComponentValue(primaryKey, name);
        return entityService.convertFromBytes(bytesValue, component.type());
    }

    /**
     * Parses PrimaryKey to human readable form
     *
     * @param primaryKey the
     * @return String in format "component_name : component_value, component_name_1 : component_value_1, etc"
     */
    public String toString(byte[] primaryKey) {
        StringBuilder builder = new StringBuilder();
        for (HFieldComponent component : components) {
            Object value = getCastedComponentValue(primaryKey, component.name());
            builder.append(component.name()).append(":").append(value.toString().trim()).append(" ");
        }

        return builder.toString();
    }
}
