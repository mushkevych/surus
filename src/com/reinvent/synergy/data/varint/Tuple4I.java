package com.reinvent.synergy.data.varint;

/**
 * @author Bohdan Mushkevych
 * Description: module manage tuple of int values: alpha and beta and gama and delta
 */
public class Tuple4I extends Tuple3I {
    protected int delta;

    public Tuple4I() {
        this.type = TupleType.TYPE_FOUR_INTEGERS;
    }

    public Tuple4I(int alpha, int beta, int gama, int delta) {
        super(alpha, beta, gama);
        this.type = TupleType.TYPE_FOUR_INTEGERS;
        this.delta = delta;
    }

    public void reset(int alpha, int beta, int gama, int delta) {
        super.reset(alpha, beta, gama);
        this.delta = delta;
    }

    /**
     * @return int presenting delta
     */
    public int getDelta() {
        return delta;
    }

    /**
     * Presents tuple in human readable form
     * @return String in format "alpha beta gama delta"
     */
    public String toString() {
        return String.format("%d %d %d %d", alpha, beta, gama, delta);
    }
}
