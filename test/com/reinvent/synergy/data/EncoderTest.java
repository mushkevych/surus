package com.reinvent.synergy.data;

import com.reinvent.synergy.data.varint.*;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * @author Bohdan Mushkevych
 * date 2011-2012
 * Description: module testing Encodings and Intermedia tuples
 */
public class EncoderTest extends TestCase{
    Encoder encoder = new Encoder();
    Tuple2I tuple2I = new Tuple2I();
    Tuple3I tuple3I = new Tuple3I();
    Tuple4I tuple4I = new Tuple4I();

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void test2I() throws Exception {
        int alpha = 20000;
        int beta = 100;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta);
        tuple2I = encoder.decode(pk.get(), tuple2I);
        assertEquals(alpha, tuple2I.getAlpha());
        assertEquals(beta, tuple2I.getBeta());

        BytesWritable bytesWritable = encoder.generateBW(alpha, beta);
        tuple2I = encoder.decode(bytesWritable.getBytes(), tuple2I);
        assertEquals(alpha, tuple2I.getAlpha());
        assertEquals(beta, tuple2I.getBeta());
    }

    public void test3I() throws Exception {
        int alpha = 20000;
        int beta = 400;
        int gama = 100;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta, gama);
        tuple3I = encoder.decode(pk.get(), tuple3I);
        assertEquals(alpha, tuple3I.getAlpha());
        assertEquals(beta, tuple3I.getBeta());
        assertEquals(gama, tuple3I.getGama());

        BytesWritable bytesWritable = encoder.generateBW(alpha, beta, gama);
        tuple3I = encoder.decode(bytesWritable.getBytes(), tuple3I);
        assertEquals(alpha, tuple3I.getAlpha());
        assertEquals(beta, tuple3I.getBeta());
        assertEquals(gama, tuple3I.getGama());
    }

    public void test4I() throws Exception {
        int alpha = 20000;
        int beta = 300;
        int gama = 200;
        int delta = 100;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta, gama, delta);
        tuple4I = encoder.decode(pk.get(), tuple4I);
        assertEquals(alpha, tuple4I.getAlpha());
        assertEquals(beta, tuple4I.getBeta());
        assertEquals(gama, tuple4I.getGama());
        assertEquals(delta, tuple4I.getDelta());

        BytesWritable bytesWritable = encoder.generateBW(alpha, beta, gama, delta);
        tuple4I = encoder.decode(bytesWritable.getBytes(), tuple4I);
        assertEquals(alpha, tuple4I.getAlpha());
        assertEquals(beta, tuple4I.getBeta());
        assertEquals(gama, tuple4I.getGama());
        assertEquals(delta, tuple4I.getDelta());
    }

    public void testEncoding() throws Exception {
        int alpha = 20000;
        int beta = 400;
        int gama = 100;
        int delta = 100;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta);
        AbstractTuple tuple = encoder.decode(pk.get());
        assertTrue(tuple.getType() == TupleType.TYPE_TWO_INTEGERS);

        pk = encoder.generateIBW(alpha, beta, gama);
        tuple = encoder.decode(pk.get());
        assertTrue(tuple.getType() == TupleType.TYPE_THREE_INTEGERS);

        pk = encoder.generateIBW(alpha, beta, gama, delta);
        tuple = encoder.decode(pk.get());
        assertTrue(tuple.getType() == TupleType.TYPE_FOUR_INTEGERS);
    }

    public void testReuse() throws Exception {
        int alpha = 20000;
        int beta = 400;
        int gama = 100;
        int delta = 100;
        int shift = 10000;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta, gama, delta);
        tuple4I = encoder.decode(pk.get(), tuple4I);
        assertEquals(alpha, tuple4I.getAlpha());
        assertEquals(beta, tuple4I.getBeta());
        assertEquals(gama, tuple4I.getGama());
        assertEquals(delta, tuple4I.getDelta());

        pk = encoder.generateIBW(alpha + shift, beta - shift, gama + shift, delta - shift);
        tuple4I = encoder.decode(pk.get(), tuple4I);
        assertEquals(alpha + shift, tuple4I.getAlpha());
        assertEquals(beta - shift, tuple4I.getBeta());
        assertEquals(gama + shift, tuple4I.getGama());
        assertEquals(delta - shift, tuple4I.getDelta());
    }

    public void testSize() throws Exception {
        int alpha = 20000;
        int beta = 400;
        int gama = 100;
        int delta = 100;

        ImmutableBytesWritable pk = encoder.generateIBW(alpha, beta);
        assertTrue(pk.get().length <= Bytes.SIZEOF_INT * 2);
        System.out.println("diff 2: " + (pk.get().length - Bytes.SIZEOF_INT * 2));

        pk = encoder.generateIBW(alpha, beta, gama);
        assertTrue(pk.get().length <= Bytes.SIZEOF_INT * 3);
        System.out.println("diff 3: " + (pk.get().length - Bytes.SIZEOF_INT * 3));

        pk = encoder.generateIBW(alpha, beta, gama, delta);
        assertTrue(pk.get().length <= Bytes.SIZEOF_INT * 4);
        System.out.println("diff 4: " + (pk.get().length - Bytes.SIZEOF_INT * 4));
    }

    public void testByteOrder() throws Exception {
        System.out.println("0 is in comparison to 1 is " + Bytes.compareTo(Bytes.toBytes(0), Bytes.toBytes(1)));
        System.out.println("1 is in comparison to 0 is " + Bytes.compareTo(Bytes.toBytes(1), Bytes.toBytes(0)));
        System.out.println("* in comparison to 0 is " + Bytes.compareTo(Bytes.toBytes('*'), Bytes.toBytes(0)));
    }


    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main("com.reinvent.synergy.data.EncoderTest");
    }
}
