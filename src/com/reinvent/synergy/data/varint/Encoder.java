package com.reinvent.synergy.data.varint;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Varint;

import java.io.*;

/**
 * @author Bohdan Mushkevych
 * Description: module contains logic for VarInt integer encoding: conversion to and from the byte array
 */
public class Encoder {

    protected ByteArrayOutputStream baos = new ByteArrayOutputStream();
    protected DataOutputStream dos = new DataOutputStream(baos);
    private Logger log = Logger.getRootLogger();

    private int alpha;
    private int beta;
    private int gama;
    private int delta;

    public Encoder() {
    }

    /**
     * Method generates byte[] for varargs of integers
     * @param args varargs of Integer type
     * @return generated value
     */
    protected byte[] getByteArray(int... args) {
        byte[] result = null;
        try {
            baos.reset();
            for (int i : args) {
                Varint.writeSignedVarInt(i, dos);
            }
            result = baos.toByteArray();
        } catch (IOException e) {
            log.error("Exception on Integer encoding", e);
        }
        return result;
    }

    /**
     * method decodes tuple from byte array and reuses existing instance to "reset" its fields with retrieved data
     * @param value encoded tuple
     * @param tuple instance to reuse
     * @return tuple instance
     */
    public <T extends AbstractTuple> T decode(byte[] value, T tuple) {
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        DataInputStream dis = new DataInputStream(bais);

        alpha = 0;
        beta = 0;
        gama = 0;
        delta = 0;
        try {
            alpha = Varint.readSignedVarInt(dis);
            beta = Varint.readSignedVarInt(dis);

            if (tuple.getType() == TupleType.TYPE_THREE_INTEGERS
                    || tuple.getType() == TupleType.TYPE_FOUR_INTEGERS) {
                gama = Varint.readSignedVarInt(dis);
            }

            if (tuple.getType() == TupleType.TYPE_FOUR_INTEGERS) {
                delta = Varint.readSignedVarInt(dis);
            }

            if (tuple.getType() == TupleType.TYPE_TWO_INTEGERS) {
                ((Tuple2I) tuple).reset(alpha, beta);
            } else if (tuple.getType() == TupleType.TYPE_THREE_INTEGERS) {
                ((Tuple3I) tuple).reset(alpha, beta, gama);
            } else if (tuple.getType() == TupleType.TYPE_FOUR_INTEGERS) {
                ((Tuple4I) tuple).reset(alpha, beta, gama, delta);
            }
        } catch (EOFException e) {
            log.error("Exception on Integer decoding", e);
        } catch (IOException e) {
            log.error("Exception on Integer decoding", e);
        }

        return tuple;
    }

    /**
     * method decodes tuple from byte array and returns tuple accordingly to number of fields in the encoded array
     * @param value encoded tuple
     * @return formed tuple instance
     */
    public AbstractTuple decode(byte[] value) {
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        DataInputStream dis = new DataInputStream(bais);

        alpha = 0;
        beta = 0;
        gama = 0;
        delta = 0;
        TupleType type = null;
        try {
            alpha = Varint.readSignedVarInt(dis);
            beta = Varint.readSignedVarInt(dis);
            type = TupleType.TYPE_TWO_INTEGERS;

            gama = Varint.readSignedVarInt(dis);
            type = TupleType.TYPE_THREE_INTEGERS;

            delta = Varint.readSignedVarInt(dis);
            type = TupleType.TYPE_FOUR_INTEGERS;

        } catch (EOFException e) {
            // empty block - we expect it
        } catch (IOException e) {
            log.error("Exception on Integer decoding", e);
        }

        if (type == TupleType.TYPE_TWO_INTEGERS) {
            return new Tuple2I(alpha, beta);
        } else if (type == TupleType.TYPE_THREE_INTEGERS) {
            return new Tuple3I(alpha, beta, gama);
        } else if (type == TupleType.TYPE_FOUR_INTEGERS) {
            return new  Tuple4I(alpha, beta, gama, delta);
        } else {
            return null;
        }
    }

    /**
     * Method generates ImmutableBytesWritable for tuple of alpha and beta
     * @param alpha the
     * @param beta the
     * @return generated value
     */
    public ImmutableBytesWritable generateIBW(int alpha, int beta) {
        return new ImmutableBytesWritable(getByteArray(alpha, beta));
    }

    /**
     * Method generates BytesWritable for tuple of alpha and beta
     * @param alpha the
     * @param beta the
     * @return generated value
     */
    public BytesWritable generateBW(int alpha, int beta) {
        return new BytesWritable(getByteArray(alpha, beta));
    }

    /**
     * Method generates ImmutableBytesWritable for tuple of alpha and beta and gama
     * @param alpha the
     * @param beta the
     * @param gama the
     * @return generated value
     */
    public ImmutableBytesWritable generateIBW(int alpha, int beta, int gama) {
        return new ImmutableBytesWritable(getByteArray(alpha, beta, gama));
    }

    /**
     * Method generates ImmutableBytesWritable for tuple of alpha and beta and gama
     * @param alpha the
     * @param beta the
     * @param gama the
     * @return generated value
     */
    public BytesWritable generateBW(int alpha, int beta, int gama) {
        return new BytesWritable(getByteArray(alpha, beta, gama));
    }

    /**
     * Method generates ImmutableBytesWritable for tuple of alpha and beta and gama
     * @param alpha the
     * @param beta the
     * @param gama the
     * @param delta the
     * @return generated value
     */
    public ImmutableBytesWritable generateIBW(int alpha, int beta, int gama, int delta) {
        return new ImmutableBytesWritable(getByteArray(alpha, beta, gama, delta));
    }

    /**
     * Method generates BytesWritable for tuple of alpha and beta and gama
     * @param alpha the
     * @param beta the
     * @param gama the
     * @param delta the
     * @return generated value
     */
    public BytesWritable generateBW(int alpha, int beta, int gama, int delta) {
        return new BytesWritable(getByteArray(alpha, beta, gama, delta));
    }

}
