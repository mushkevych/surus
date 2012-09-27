package com.reinvent.synergy.data.varint;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * @author Bohdan Mushkevych
 * Description: module manage tuple of int values: alpha and beta and gama
 */
public class Tuple3I extends Tuple2I {
    protected int gama;

    public Tuple3I() {
        this.type = TupleType.TYPE_THREE_INTEGERS;
    }

    public Tuple3I(int alpha, int beta, int gama) {
        super(alpha, beta);
        this.type = TupleType.TYPE_THREE_INTEGERS;
        this.gama = gama;
    }

    public void reset(int alpha, int beta, int gama) {
        super.reset(alpha, beta);
        this.gama = gama;
    }

    /**
     * Parses Value and extracts gama component
     * @return int presenting gama
     */
    public int getGama() {
        return gama;
    }

    /**
     * Presents tuple in human readable form
     * @return String in format "alpha beta gama"
     */
    public String toString() {
        return String.format("%d %d %d", alpha, beta, gama);
    }
}
