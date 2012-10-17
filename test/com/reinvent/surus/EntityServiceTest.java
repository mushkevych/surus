package com.reinvent.surus;

import com.reinvent.surus.mapping.*;
import com.reinvent.surus.model.Constants;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.util.*;

/**
 * @author Bohdan Mushkevych
 * Description: UT covers marshaling/unmarshaling for the ORM core - EntityService class
 */
public class EntityServiceTest extends TestCase {
    public static class TestModel {
        @HRowKey(components = {
                 @HFieldComponent(name = Constants.KEY, length = HFieldComponent.LENGTH_VARIABLE, type = byte[].class)
        })
        public byte[] key;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_STRING)
        public String fieldString;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_INTEGER)
        public int fieldInteger;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LONG)
        public long fieldLong;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_DOUBLE)
        public double fieldDouble;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_BOOLEAN)
        public boolean fieldBoolean;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_FLOAT)
        public float fieldFloat;

        @HProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_BYTE_ARRAY)
        public byte[] fieldByteArray;

        @HListProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LIST_S, elementType = String.class)
        public List<String> fieldListS;

        @HListProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LIST_D, elementType = Double.class)
        public List<Double> fieldListD;

        @HListProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LIST_I, elementType = Integer.class)
        public List<Integer> fieldListI;

        @HListProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LIST_L, elementType = Long.class)
        public List<Long> fieldListL;

        @HListProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_LIST_B, elementType = byte[].class)
        public List<byte[]> fieldListB;

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_SI,
                keyType = String.class, valueType = Integer.class)
        public Map<String, Integer> fieldMapSI = new HashMap<String, Integer>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_SS,
                keyType = String.class, valueType = String.class)
        public Map<String, String> fieldMapSS = new HashMap<String, String>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_SD,
                keyType = String.class, valueType = Double.class)
        public Map<String, Double> fieldMapSD = new HashMap<String, Double>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_SB,
                keyType = String.class, valueType = byte[].class)
        public Map<String, byte[]> fieldMapSB = new HashMap<String, byte[]>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_II,
                keyType = Integer.class, valueType = Integer.class)
        public Map<Integer, Integer> fieldMapII = new HashMap<Integer, Integer>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_IB,
                keyType = Integer.class, valueType = byte[].class)
        public Map<Integer, byte[]> fieldMapIB = new HashMap<Integer, byte[]>();

        @HMapProperty(family = Constants.FAMILY_STAT, identifier = TestConstants.FIELD_MAP_LI,
                keyType = Long.class, valueType = Integer.class)
        public Map<Long, Integer> fieldMapLI = new HashMap<Long, Integer>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SSI, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = String.class, valueType = Integer.class)
        public Map<String, Map<String, Integer>> ssi = new HashMap<String, Map<String, Integer>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SSB, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = String.class, valueType = byte[].class)
        public Map<String, Map<String, byte[]>> ssb = new HashMap<String, Map<String, byte[]>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SSD, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = String.class, valueType = Double.class)
        public Map<String, Map<String, Double>> ssd = new HashMap<String, Map<String, Double>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SSS, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = String.class, valueType = String.class)
        public Map<String, Map<String, String>> sss = new HashMap<String, Map<String, String>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SIB, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = Integer.class, valueType = byte[].class)
        public Map<String, Map<Integer, byte[]>> sib = new HashMap<String, Map<Integer, byte[]>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SII, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = Integer.class, valueType = Integer.class)
        public Map<String, Map<Integer, Integer>> sii = new HashMap<String, Map<Integer, Integer>>();

        @HMapFamily(family = TestConstants.FAMILY_MAP_SLI, keyType = String.class, valueType = Map.class)
        @HNestedMap(keyType = Long.class, valueType = Integer.class)
        public Map<String, Map<Long, Integer>> sli = new HashMap<String, Map<Long, Integer>>();


        public TestModel() {
        }
    }

    EntityServiceTest.TestModel testModel;
    EntityService<EntityServiceTest.TestModel> esTestModel = new EntityService<EntityServiceTest.TestModel>(EntityServiceTest.TestModel.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();

        testModel = new EntityServiceTest.TestModel();
        testModel.key = Bytes.toBytes(TestConstants.DEFAULT_STRING);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        testModel = null;
    }

    public void testMarshalingCycle() throws Exception {
        testModel.fieldBoolean = TestConstants.DEFAULT_BOOLEAN;
        testModel.fieldByteArray = TestConstants.DEFAULT_BYTE_ARRAY;
        testModel.fieldString = TestConstants.DEFAULT_STRING;
        testModel.fieldDouble = TestConstants.DEFAULT_DOUBLE;
        testModel.fieldFloat = TestConstants.DEFAULT_FLOAT;
        testModel.fieldInteger = TestConstants.DEFAULT_INTEGER;
        testModel.fieldLong = TestConstants.DEFAULT_LONG;

        testModel.fieldListS = UnitTestHelper.generateListField(String.class);
        testModel.fieldListD = UnitTestHelper.generateListField(Double.class);
        testModel.fieldListB = UnitTestHelper.generateListField(byte[].class);
        testModel.fieldListI = UnitTestHelper.generateListField(Integer.class);
        testModel.fieldListL = UnitTestHelper.generateListField(Long.class);

        testModel.fieldMapIB = UnitTestHelper.generateMapField(Integer.class, byte[].class);
        testModel.fieldMapII = UnitTestHelper.generateMapField(Integer.class, Integer.class);
        testModel.fieldMapLI = UnitTestHelper.generateMapField(Long.class, Integer.class);
        testModel.fieldMapSB = UnitTestHelper.generateMapField(String.class, byte[].class);
        testModel.fieldMapSD = UnitTestHelper.generateMapField(String.class, Double.class);
        testModel.fieldMapSI = UnitTestHelper.generateMapField(String.class, Integer.class);
        testModel.fieldMapSS = UnitTestHelper.generateMapField(String.class, String.class);

        testModel.ssi = UnitTestHelper.generateComplexMap(String.class, String.class, Integer.class);
        testModel.ssd = UnitTestHelper.generateComplexMap(String.class, String.class, Double.class);
        testModel.ssb = UnitTestHelper.generateComplexMap(String.class, String.class, byte[].class);
        testModel.sss = UnitTestHelper.generateComplexMap(String.class, String.class, String.class);
        testModel.sib = UnitTestHelper.generateComplexMap(String.class, Integer.class, byte[].class);
        testModel.sii = UnitTestHelper.generateComplexMap(String.class, Integer.class, Integer.class);
        testModel.sli = UnitTestHelper.generateComplexMap(String.class, Long.class, Integer.class);

        Put put = esTestModel.insert(testModel);
        Result result = UnitTestHelper.putToResult(put);
        TestModel unmarshaled = esTestModel.parseResult(result);

        assertEquals(testModel.fieldBoolean, unmarshaled.fieldBoolean);
        assertEquals(testModel.fieldString, unmarshaled.fieldString);
        assertEquals(testModel.fieldDouble, unmarshaled.fieldDouble);
        assertEquals(testModel.fieldFloat, unmarshaled.fieldFloat);
        assertEquals(testModel.fieldInteger, unmarshaled.fieldInteger);
        assertEquals(testModel.fieldLong, unmarshaled.fieldLong);
        Assert.assertArrayEquals(testModel.fieldByteArray, unmarshaled.fieldByteArray);

        assertTrue(testModel.fieldListS.equals(unmarshaled.fieldListS));
        assertTrue(testModel.fieldListD.equals(unmarshaled.fieldListD));
        assertTrue(testModel.fieldListI.equals(unmarshaled.fieldListI));
        assertTrue(testModel.fieldListL.equals(unmarshaled.fieldListL));
        UnitTestHelper.equalsLists(testModel.fieldListB, unmarshaled.fieldListB, byte[].class);

        assertTrue(testModel.fieldMapII.equals(unmarshaled.fieldMapII));
        assertTrue(testModel.fieldMapLI.equals(unmarshaled.fieldMapLI));
        assertTrue(testModel.fieldMapSD.equals(unmarshaled.fieldMapSD));
        assertTrue(testModel.fieldMapSI.equals(unmarshaled.fieldMapSI));
        assertTrue(testModel.fieldMapSS.equals(unmarshaled.fieldMapSS));
        UnitTestHelper.equalsMaps(testModel.fieldMapIB, unmarshaled.fieldMapIB, byte[].class);
        UnitTestHelper.equalsMaps(testModel.fieldMapSB, unmarshaled.fieldMapSB, byte[].class);

        assertTrue(testModel.ssi.equals(unmarshaled.ssi));
        assertTrue(testModel.ssd.equals(unmarshaled.ssd));
        assertTrue(testModel.sss.equals(unmarshaled.sss));
        assertTrue(testModel.sli.equals(unmarshaled.sli));
        assertTrue(testModel.sii.equals(unmarshaled.sii));

        UnitTestHelper.equalsComplexMap(testModel.sib, unmarshaled.sib, byte[].class);
        UnitTestHelper.equalsComplexMap(testModel.ssb, unmarshaled.ssb, byte[].class);
    }



    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main("com.reinvent.surus.EntityServiceTest");
    }
}
