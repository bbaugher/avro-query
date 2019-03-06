package com.bb021949.query.selector;

import org.apache.avro.generic.GenericData;

/**
 * Selector that accepts all data
 */
public class AllSelector implements Selector {
    private static final long serialVersionUID = -2495932538167026704L;

    @Override
    public boolean select(GenericData.Record record) {
        return true;
    }

    @Override public String toString() {
        return "AllSelector{}";
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof AllSelector;
    }
}
