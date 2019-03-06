package com.bb021949.query.plan;

import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link GenericData.Record} into {@link List<Pair<String, String>>} where each pair of
 * strings is a field name and field value.
 */
public class GenericRecordTableMapFn extends MapFn<GenericData.Record, Pair<List<Pair<String, String>>,
        GenericData.Record>> {

    private static final long serialVersionUID = 4575622430370800834L;
    private final List<String> fields;

    public GenericRecordTableMapFn(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public Pair<List<Pair<String, String>>, GenericData.Record> map(GenericData.Record record) {
        return Pair.of(getKey(record), record);
    }

    private List<Pair<String, String>> getKey(GenericData.Record record) {
        List<Pair<String, String>> pairs = new ArrayList<>();
        for (String field : fields) {
            Object value = record.get(field);
            String stringValue = value == null ? "null" : value.toString();
            pairs.add(Pair.of(field, stringValue));
        }
        return pairs;
    }
}
