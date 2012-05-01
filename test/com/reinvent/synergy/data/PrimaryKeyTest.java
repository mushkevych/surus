package com.reinvent.synergy.data;

import com.reinvent.synergy.data.primarykey.IntegerPrimaryKey;
import com.reinvent.synergy.data.primarykey.StringPrimaryKey;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * @author Bohdan Mushkevych
 * date 11/10/11
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

    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main("com.reinvent.synergy.data.PrimaryKeyTest");
    }
}
