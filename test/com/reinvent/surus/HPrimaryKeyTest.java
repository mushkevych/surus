package com.reinvent.surus;

import com.reinvent.surus.mapping.EntityService;
import com.reinvent.surus.mapping.HFieldComponent;
import com.reinvent.surus.model.ExampleComplex;
import com.reinvent.surus.model.Constants;
import com.reinvent.surus.model.Example;
import com.reinvent.surus.primarykey.HPrimaryKey;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * Description: UT for HPrimaryKey primary keys: one single-component, other multi-component
 */
public class HPrimaryKeyTest extends TestCase {
    HPrimaryKey<Example> pkExample = new HPrimaryKey<Example>(Example.class, new EntityService<Example>(Example.class));
    HPrimaryKey<ExampleComplex> pkComplexExample = new HPrimaryKey<ExampleComplex>(ExampleComplex.class, new EntityService<ExampleComplex>(ExampleComplex.class));

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimpleComponents() throws Exception {
        HFieldComponent[] components = pkExample.getComponents();

        assertEquals(1, components.length);
        assertEquals(Constants.KEY, components[0].name());
        assertEquals(HFieldComponent.LENGTH_VARIABLE, components[0].length());
        assertEquals(byte[].class, components[0].type());
    }

    public void testSimpleComponentsByName() throws Exception {
        HFieldComponent component = pkExample.getComponent(Constants.KEY);

        assertEquals(Constants.KEY, component.name());
        assertEquals(HFieldComponent.LENGTH_VARIABLE, component.length());
        assertEquals(byte[].class, component.type());
    }

    public void testSimpleGenerate() throws Exception {
        String key = "String Key 123";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.KEY, key);

        byte[] rowKey = pkExample.generateRowKey(components).get();
        String casted = Bytes.toString((byte[]) pkExample.getCastedComponentValue(rowKey, Constants.KEY));

        assertEquals(key, casted);
    }

    public void testSimpleToString() throws Exception {
        String key = "String Key 123";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.KEY, key);

        byte[] rowKey = pkExample.generateRowKey(components).get();
        System.out.println(pkExample.toString(rowKey));
    }


    public void testComplexComponents() throws Exception {
        HFieldComponent[] components = pkComplexExample.getComponents();

        assertEquals(2, components.length);

        assertEquals(Constants.TIMEPERIOD, pkComplexExample.getComponent(Constants.TIMEPERIOD).name());
        assertEquals(Bytes.SIZEOF_INT, pkComplexExample.getComponent(Constants.TIMEPERIOD).length());
        assertEquals(Integer.class, pkComplexExample.getComponent(Constants.TIMEPERIOD).type());

        assertEquals(Constants.DOMAIN_NAME, pkComplexExample.getComponent(Constants.DOMAIN_NAME).name());
        assertEquals(64, pkComplexExample.getComponent(Constants.DOMAIN_NAME).length());
        assertEquals(String.class, pkComplexExample.getComponent(Constants.DOMAIN_NAME).type());
    }

    public void testComplexGenerate() throws Exception {
        Integer timeperiod = 2010010200;
        String domainName = "domain_name as name";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.TIMEPERIOD, timeperiod);
        components.put(Constants.DOMAIN_NAME, domainName);

        byte[] rowKey = pkComplexExample.generateRowKey(components).get();
        String castedStr = (String) pkComplexExample.getCastedComponentValue(rowKey, Constants.DOMAIN_NAME);
        Integer castedInt = (Integer) pkComplexExample.getCastedComponentValue(rowKey, Constants.TIMEPERIOD);

        assertEquals(domainName, castedStr.trim());
        assertEquals(timeperiod, castedInt);
    }

    public void testComplexGenerateWithOverflow() throws Exception {
        Integer timeperiod = 2010010200;
        String domainName = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.TIMEPERIOD, timeperiod);
        components.put(Constants.DOMAIN_NAME, domainName);

        byte[] rowKey = pkComplexExample.generateRowKey(components).get();
        String castedStr = (String) pkComplexExample.getCastedComponentValue(rowKey, Constants.DOMAIN_NAME);
        Integer castedInt = (Integer) pkComplexExample.getCastedComponentValue(rowKey, Constants.TIMEPERIOD);

        assertEquals(domainName.substring(0, 64), castedStr.trim());
        assertEquals(timeperiod, castedInt);
    }

    public void testComplexToString() throws Exception {
        Integer timeperiod = 2010010200;
        String domainName = "domain_name as name";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.TIMEPERIOD, timeperiod);
        components.put(Constants.DOMAIN_NAME, domainName);

        byte[] rowKey = pkComplexExample.generateRowKey(components).get();
        System.out.println(pkComplexExample.toString(rowKey));
    }

    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main("com.reinvent.surus.HPrimaryKeyTest");
    }
}
