package com.bb021949.query.plan;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts {@link GenericData.Record} into {@link Map<String, String>} where each key/value is a
 * record's field name and field value.
 */
public class RecordToRowListMapFn extends MapFn<GenericData.Record, Map<String, String>> {

    @Override
    public Map<String, String> map(GenericData.Record record) {
        Map<String, String> row = new HashMap<>();

        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            row.put(field.name(), value == null ? "null" : value.toString());
        }

        return row;
    }
}
