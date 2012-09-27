package com.reinvent.synergy.data.varint;

/**
 * @author Bohdan Mushkevych
 * Description: module presents tuple of two int values: alpha and beta
 */
public class Tuple2I extends AbstractTuple {
    protected int alpha;
    protected int beta;

    public Tuple2I() {
        this.type = TupleType.TYPE_TWO_INTEGERS;
    }

    public Tuple2I(int alpha, int beta) {
        this.type = TupleType.TYPE_TWO_INTEGERS;
        this.alpha = alpha;
        this.beta = beta;
    }

    public void reset(int alpha, int beta) {
        this.alpha = alpha;
        this.beta = beta;
    }

    /**
     * @return int presenting alpha
     */
    public int getAlpha() {
        return alpha;
    }

    /**
     * @return int presenting beta
     */
    public int getBeta() {
        return beta;
    }

    /**
     * Presents tuple in human readable form
     * @return String in format "alpha beta"
     */
    public String toString() {
        return String.format("%d %d", alpha, beta);
    }
}
