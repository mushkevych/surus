package com.reinvent.synergy.data.varint;

/**
 * @author Bohdan Mushkevych
 * date 07 March 2012
 * Description:
 */
public class AbstractTuple {
    protected TupleType type;

    public void setType(TupleType type) {
        this.type = type;
    }

    public TupleType getType() {
        return type;
    }
}
