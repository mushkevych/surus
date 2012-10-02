package com.reinvent.surus;

import com.reinvent.surus.model.Constants;
import com.reinvent.surus.primarykey.IntegerPrimaryKey;
import com.reinvent.surus.primarykey.StringPrimaryKey;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * Description: UT for integer and string primary keys
 */
public class PrimaryKeyTest extends TestCase {
    IntegerPrimaryKey pkInteger = new IntegerPrimaryKey();
    StringPrimaryKey pkString = new StringPrimaryKey();

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testPkInteger() throws Exception {
        int value = 1000;
        ImmutableBytesWritable pk = pkInteger.generateKey(value);

        assertEquals(value, (int) pkInteger.getValue(pk.get()));
        assertNotNull(pkInteger.toString(pk.get()));
//        System.out.println(pkInteger.toString(pk.get()));
    }

    public void testPkString() throws Exception {
        String value = "a.com";

        ImmutableBytesWritable pk = pkString.generateKey(value);
        assertEquals(value, pkString.toString(pk.get()));
//        System.out.println(pkString.toString(pk.get()));
    }

    public void testStringKeyComponents() throws Exception {
        String key = "string key";
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.KEY, key);

        byte[] pk = pkString.generateRowKey(components).get();
        assertTrue(key.equals(pkString.toString(pk)));
    }

    public void testIntegerKeyComponents() throws Exception {
        Integer key = 987654321;
        Map<String, Object> components = new HashMap<String, Object>();
        components.put(Constants.KEY, key);

        byte[] pk = pkInteger.generateRowKey(components).get();
        assertTrue(key.equals(pkInteger.getValue(pk)));
    }

    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main("com.reinvent.surus.PrimaryKeyTest");
    }
}
