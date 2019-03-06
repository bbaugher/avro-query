package com.bb021949.query.selector;

import org.apache.avro.generic.GenericData;

import java.util.Objects;

/**
 * {@link Selector} for inverting results of another {@link Selector}
 */
public class InverseSelector implements Selector {

    private static final long serialVersionUID = 6408513470875283002L;
    private final Selector selector;

    /**
     * Creates the selector
     *
     * @param selector
     *      the other selector to invert results of
     */
    public InverseSelector(Selector selector) {
        this.selector = selector;
    }

    @Override
    public boolean select(GenericData.Record record) {
        return !selector.select(record);
    }

    @Override
    public String toString() {
        return "InverseSelector{" + "selector=" + selector + '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InverseSelector that = (InverseSelector) o;
        return Objects.equals(selector, that.selector);
    }

    @Override public int hashCode() {
        return Objects.hash(selector);
    }
}
