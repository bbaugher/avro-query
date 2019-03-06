package com.bb021949.query.selector;

import org.apache.avro.generic.GenericData;

import java.io.Serializable;

/**
 * Interface used to define a select query.
 */
public interface Selector extends Serializable {
    /**
     * If the {@link Selector} wants to keep this data
     *
     * @param record
     *      the record to consider selecting or filtering
     * @return true if the record should be kept and false if it should be filtered out
     */
    boolean select(GenericData.Record record);
}
